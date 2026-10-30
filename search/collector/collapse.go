//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"fmt"
	"sort"

	"github.com/blugelabs/bluge/search"
)

// CollapsingTopNCollector 在普通 TopN 采集之上提供字段折叠能力：
// 先在全部匹配文档中按折叠字段（单值 keyword 字段）选出每组排序最靠前的
// 代表文档，再对代表文档集合应用数量、skip、After 和 Before 窗口。
// 聚合仍然基于全部原始匹配文档计算。
//
// 分组状态只存在于单次 Collect 调用期间，Collect 返回（无论成功或失败）
// 后不会残留任何可影响后续搜索的状态。
type CollapsingTopNCollector struct {
	size    int
	skip    int
	sort    search.SortOrder
	after   [][]byte
	reverse bool

	collapseField string

	results     search.DocumentMatchCollection
	backingSize int

	neededFields []string
}

// NewCollapsingTopNCollector 构造一个按 collapseField 折叠的 TopN 采集器。
// collapseField 必须是映射中已开启 Sortable（具备 doc values）的单值
// keyword 字段，无需出现在 sortOrder 中；sortOrder 始终为请求声明的原始
// 排序：组代表依据它选出，reverse 仅用于 Before 游标取数，不改变组代表
// 的选择。after 非空时启用游标分页；reverse 为 true 时表示 Before 语义。
func NewCollapsingTopNCollector(size, skip int, sortOrder search.SortOrder,
	after [][]byte, reverse bool, collapseField string) *CollapsingTopNCollector {
	hc := &CollapsingTopNCollector{
		size:          size,
		skip:          skip,
		sort:          sortOrder,
		after:         after,
		reverse:       reverse,
		collapseField: collapseField,
	}

	// 与 TopNCollector 相同的预分配封顶策略；折叠需要遍历全部匹配，
	// 不足部分由 DocumentMatchPool 按需扩容。
	hc.backingSize = size + skip + 1
	if size+skip > PreAllocSizeSkipCap {
		hc.backingSize = PreAllocSizeSkipCap + 1
	}

	hc.neededFields = append(hc.neededFields, sortOrder.Fields()...)
	hc.neededFields = appendUniqueField(hc.neededFields, collapseField)

	return hc
}

func appendUniqueField(fields []string, field string) []string {
	for _, existing := range fields {
		if existing == field {
			return fields
		}
	}
	return append(fields, field)
}

func (hc *CollapsingTopNCollector) Size() int {
	sizeInBytes := reflectStaticSizeCollapsingTopNCollector + sizeOfPtr

	for _, entry := range hc.neededFields {
		sizeInBytes += len(entry) + sizeOfString
	}

	return sizeInBytes
}

func (hc *CollapsingTopNCollector) BackingSize() int {
	return hc.backingSize
}

// Collect 遍历全部匹配文档，完成折叠、聚合与窗口截取
func (hc *CollapsingTopNCollector) Collect(ctx context.Context, aggs search.Aggregations,
	searcher search.Collectible) (search.DocumentMatchIterator, error) {
	// 即使出错也必须关闭 searcher
	defer func() {
		_ = searcher.Close()
	}()

	searchContext := search.NewSearchContext(hc.backingSize+searcher.DocumentMatchPoolSize(), len(hc.sort))

	// 追加聚合所需字段
	hc.neededFields = append(hc.neededFields, aggs.Fields()...)
	bucket := search.NewBucket("", aggs)

	// 以下分组状态全部为本次 Collect 局部状态，失败即随采集器丢弃
	var (
		all        search.DocumentMatchCollection
		standalone search.DocumentMatchCollection
		groups     = make(map[string]*search.DocumentMatch)
	)

	var hitNumber int
	var next *search.DocumentMatch
	var err error

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		next, err = searcher.Next(searchContext)
	}
	for err == nil && next != nil {
		if hitNumber%CheckDoneEvery == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}

		hitNumber++
		next.HitNumber = hitNumber

		err = hc.collectSingle(searchContext, next, bucket, &all, &standalone, groups)
		if err != nil {
			return nil, err
		}

		next, err = searcher.Next(searchContext)
	}
	if err != nil {
		return nil, err
	}

	bucket.Finish()

	hc.results = hc.finalizeResults(searchContext, all, standalone, groups)

	rv := &TopNIterator{
		results: hc.results,
		bucket:  bucket,
		index:   0,
		err:     nil,
	}
	return rv, nil
}

// collectSingle 为每个匹配文档装载所需字段、计算排序值、喂给聚合，
// 并依据折叠字段更新分组代表
func (hc *CollapsingTopNCollector) collectSingle(searchContext *search.Context, d *search.DocumentMatch,
	bucket *search.Bucket, all, standalone *search.DocumentMatchCollection,
	groups map[string]*search.DocumentMatch) error {
	if len(hc.neededFields) > 0 {
		if err := d.LoadDocumentValues(searchContext, hc.neededFields); err != nil {
			return err
		}
	}

	// 按原始排序计算该命中的排序值
	hc.sort.Compute(d)

	// 聚合始终消费每一个原始匹配文档
	bucket.Consume(d)

	*all = append(*all, d)

	keyValues := d.DocValues(hc.collapseField)
	if len(keyValues) == 0 {
		// 不含折叠字段的文档各自保留
		*standalone = append(*standalone, d)
		return nil
	}
	if len(keyValues) > 1 {
		return fmt.Errorf("折叠字段 %q 在文档 %d 上存在多个值；CollapseBy 仅支持单值 keyword 字段",
			hc.collapseField, d.Number)
	}

	key := string(keyValues[0])
	existing, ok := groups[key]
	if !ok {
		groups[key] = d
		return nil
	}
	// 代表为该组内在原始排序下排在最前的变体
	if hc.sort.Compare(d, existing) < 0 {
		groups[key] = d
	}
	return nil
}

// finalizeResults 汇总全部组代表，按原始排序排序后应用游标、from 与数量窗口
func (hc *CollapsingTopNCollector) finalizeResults(searchContext *search.Context,
	all, standalone search.DocumentMatchCollection,
	groups map[string]*search.DocumentMatch) search.DocumentMatchCollection {
	representatives := make(search.DocumentMatchCollection, 0, len(standalone)+len(groups))
	representatives = append(representatives, standalone...)
	for _, representative := range groups {
		representatives = append(representatives, representative)
	}

	// 排序构成全序（HitNumber 用于打破排序键完全相同的并列）
	sort.SliceStable(representatives, func(i, j int) bool {
		return hc.sort.Compare(representatives[i], representatives[j]) < 0
	})

	window := hc.applyWindow(representatives)

	// 回收所有不属于返回窗口的文档
	returned := make(map[*search.DocumentMatch]struct{}, len(window))
	for _, d := range window {
		returned[d] = struct{}{}
	}
	for _, d := range all {
		if _, ok := returned[d]; !ok {
			searchContext.DocumentMatchPool.Put(d)
		}
	}

	// 完成最终返回给调用方的命中
	for _, d := range window {
		d.Complete(nil)
	}

	return window
}

// applyWindow 对已按原始排序排好序的代表文档应用 After/Before、from 与 size。
// After：保留排序键严格大于游标的代表，跳过 from，取 size 个；
// Before：保留排序键严格小于游标的代表，取紧邻游标之前的 size 个，
// 输出顺序仍为原始排序顺序（与 TopNCollector 的 reverse 行为一致）。
func (hc *CollapsingTopNCollector) applyWindow(
	representatives search.DocumentMatchCollection) search.DocumentMatchCollection {
	filtered := representatives
	if hc.after != nil {
		cursor := &search.DocumentMatch{
			SortValue: hc.after,
		}
		filtered = make(search.DocumentMatchCollection, 0, len(representatives))
		for _, representative := range representatives {
			// 排序键与游标完全相同者排除：让游标临时复用候选的
			// HitNumber，使全键并列时比较结果为相等
			cursor.HitNumber = representative.HitNumber
			cmp := hc.sort.Compare(representative, cursor)
			if hc.reverse {
				if cmp < 0 {
					filtered = append(filtered, representative)
				}
			} else if cmp > 0 {
				filtered = append(filtered, representative)
			}
		}
	}

	if hc.reverse {
		// TopNSearch 中 Before 不会与 from 组合，这里仅保持算术有定义：
		// 优先跳过离游标最远的文档
		end := len(filtered) - hc.skip
		if end < 0 {
			end = 0
		}
		start := end - hc.size
		if start < 0 {
			start = 0
		}
		if end > len(filtered) {
			end = len(filtered)
		}
		return filtered[start:end]
	}

	start := hc.skip
	if hc.after != nil {
		// TopNSearch 中游标分页不与 from 组合
		start = 0
	}
	end := start + hc.size
	if start > len(filtered) {
		start = len(filtered)
	}
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[start:end]
}
