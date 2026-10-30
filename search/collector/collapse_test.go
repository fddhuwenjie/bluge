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
	"reflect"
	"testing"

	segment "github.com/blugelabs/bluge_segment_api"

	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/aggregations"
)

// collapseStubDoc 描述一个待采集的匹配文档及其 doc values
type collapseStubDoc struct {
	number  uint64
	score   float64
	docVals map[string][][]byte
}

// collapseMatchReader 为单元测试提供内存 doc values
type collapseMatchReader struct {
	docVals map[string][][]byte
}

func (mr *collapseMatchReader) DocumentValueReader(fields []string) (segment.DocumentValueReader, error) {
	return mr, nil
}

func (mr *collapseMatchReader) VisitDocumentValues(number uint64, visitor segment.DocumentValueVisitor) error {
	for field, terms := range mr.docVals {
		for _, term := range terms {
			visitor(field, term)
		}
	}
	return nil
}

func (mr *collapseMatchReader) VisitStoredFields(number uint64, visitor segment.StoredFieldVisitor) error {
	return nil
}

// collapseStubSearcher 按固定顺序吐出测试文档
type collapseStubSearcher struct {
	index int
	docs  []collapseStubDoc
}

func (ss *collapseStubSearcher) Next(ctx *search.Context) (*search.DocumentMatch, error) {
	if ss.index < len(ss.docs) {
		rv := ctx.DocumentMatchPool.Get()
		doc := ss.docs[ss.index]
		rv.Number = doc.number
		rv.Score = doc.score
		rv.SetReader(&collapseMatchReader{docVals: doc.docVals})
		ss.index++
		return rv, nil
	}
	return nil, nil
}

func (ss *collapseStubSearcher) DocumentMatchPoolSize() int {
	return 0
}

func (ss *collapseStubSearcher) Close() error {
	return nil
}

// collapseVariantDocs 构造商品变体场景：
// 分数降序、_id 升序排序后，组代表依次为
// p1-a(9)、p2-a(8)、solo-1(7)、solo-2(6)、p3-a(2)
// 其中 solo-1/solo-2 没有 product 字段，应各自保留
func collapseVariantDocs() []collapseStubDoc {
	keyword := func(v string) [][]byte { return [][]byte{[]byte(v)} }
	return []collapseStubDoc{
		{number: 1, score: 1, docVals: map[string][][]byte{
			"_id": keyword("p1-c"), "product": keyword("P1")}},
		{number: 2, score: 9, docVals: map[string][][]byte{
			"_id": keyword("p1-a"), "product": keyword("P1")}},
		{number: 3, score: 5, docVals: map[string][][]byte{
			"_id": keyword("p1-b"), "product": keyword("P1")}},
		{number: 4, score: 4, docVals: map[string][][]byte{
			"_id": keyword("p2-b"), "product": keyword("P2")}},
		{number: 5, score: 8, docVals: map[string][][]byte{
			"_id": keyword("p2-a"), "product": keyword("P2")}},
		{number: 6, score: 7, docVals: map[string][][]byte{
			"_id": keyword("solo-1")}},
		{number: 7, score: 2, docVals: map[string][][]byte{
			"_id": keyword("p3-a"), "product": keyword("P3")}},
		{number: 8, score: 6, docVals: map[string][][]byte{
			"_id": keyword("solo-2")}},
	}
}

func collapseSortOrder() search.SortOrder {
	return search.SortOrder{
		search.SortBy(search.DocumentScore()).Desc(),
		search.SortBy(search.Field("_id")),
	}
}

func collapseAggs() search.Aggregations {
	aggs := make(search.Aggregations)
	aggs.Add("count", aggregations.CountMatches())
	aggs.Add("max_score", aggregations.Max(search.DocumentScore()))
	aggs.Add("products", aggregations.NewTermsAggregation(search.Field("product"), 10))
	return aggs
}

// collectCollapsed 执行一次折叠采集并返回结果编号、_id 与聚合桶
func collectCollapsed(t *testing.T, size, from int, after [][]byte, reverse bool,
	ctx context.Context) ([]uint64, []string, *search.Bucket) {
	t.Helper()

	searcher := &collapseStubSearcher{docs: collapseVariantDocs()}
	hc := NewCollapsingTopNCollector(size, from, collapseSortOrder(), after, reverse, "product")
	dmi, err := hc.Collect(ctx, collapseAggs(), searcher)
	if err != nil {
		t.Fatalf("折叠采集失败: %v", err)
	}

	var numbers []uint64
	var ids []string
	result, err := dmi.Next()
	for result != nil && err == nil {
		numbers = append(numbers, result.Number)
		if vals := result.DocValues("_id"); len(vals) > 0 {
			ids = append(ids, string(vals[0]))
		} else {
			ids = append(ids, "")
		}
		result, err = dmi.Next()
	}
	if err != nil {
		t.Fatalf("遍历结果失败: %v", err)
	}
	return numbers, ids, dmi.Aggregations()
}

func TestCollapsingCollectorBasic(t *testing.T) {
	numbers, ids, bucket := collectCollapsed(t, 10, 0, nil, false, context.Background())

	expectIDs := []string{"p1-a", "p2-a", "solo-1", "solo-2", "p3-a"}
	if !reflect.DeepEqual(ids, expectIDs) {
		t.Fatalf("期望折叠后代表顺序 %v，实际 %v", expectIDs, ids)
	}
	// 代表必须是组内排序最靠前的变体：P1 选中 2 号(p1-a)，P2 选中 5 号(p2-a)
	expectNumbers := []uint64{2, 5, 6, 8, 7}
	if !reflect.DeepEqual(numbers, expectNumbers) {
		t.Fatalf("期望代表文档编号 %v，实际 %v", expectNumbers, numbers)
	}

	// 聚合仍统计全部 8 个原始匹配文档
	if total := bucket.Count(); total != 8 {
		t.Errorf("期望聚合 count 为 8（全部原始匹配），实际 %d", total)
	}
	// terms 聚合按原始变体计数：P1=3、P2=2、P3=1
	buckets := bucket.Buckets("products")
	counts := make(map[string]uint64, len(buckets))
	for _, b := range buckets {
		counts[b.Name()] = b.Count()
	}
	expectCounts := map[string]uint64{"P1": 3, "P2": 2, "P3": 1}
	if !reflect.DeepEqual(counts, expectCounts) {
		t.Errorf("期望 terms 聚合计数 %v，实际 %v", expectCounts, counts)
	}
}

func TestCollapsingCollectorPaginationByFrom(t *testing.T) {
	seen := make(map[string]struct{})
	var pageSizes []int

	for from := 0; from < 6; from += 2 {
		_, ids, bucket := collectCollapsed(t, 2, from, nil, false, context.Background())
		pageSizes = append(pageSizes, len(ids))
		if total := bucket.Count(); total != 8 {
			t.Errorf("from=%d 时期望聚合 count 为 8，实际 %d", from, total)
		}
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				t.Errorf("from=%d 时文档 %s 重复出现在不同页", from, id)
			}
			seen[id] = struct{}{}
		}
	}

	// 每页 2 条，三页长度应为 2、2、1，共 5 个组代表
	if !reflect.DeepEqual(pageSizes, []int{2, 2, 1}) {
		t.Fatalf("期望各页大小 [2 2 1]，实际 %v", pageSizes)
	}
	expectAll := []string{"p1-a", "p2-a", "solo-1", "solo-2", "p3-a"}
	var all []string
	for _, id := range expectAll {
		if _, ok := seen[id]; ok {
			all = append(all, id)
		}
	}
	if !reflect.DeepEqual(all, expectAll) {
		t.Fatalf("翻页后代表集合不一致，期望 %v，实际 %v", expectAll, all)
	}
}

func TestCollapsingCollectorAfterCursor(t *testing.T) {
	// 第一页：取前 2 个代表，用最后一个代表的排序值作为 After 游标
	_, page1, _ := collectCollapsed(t, 2, 0, nil, false, context.Background())
	if !reflect.DeepEqual(page1, []string{"p1-a", "p2-a"}) {
		t.Fatalf("第一页期望 [p1-a p2-a]，实际 %v", page1)
	}
	cursor1 := bucketRepresentativeSortValue(t, 5)

	// 第二页必须重新在全部匹配中折叠，游标只作用于代表集合
	_, page2, _ := collectCollapsed(t, 2, 0, cursor1, false, context.Background())
	if !reflect.DeepEqual(page2, []string{"solo-1", "solo-2"}) {
		t.Fatalf("After 第二页期望 [solo-1 solo-2]，实际 %v", page2)
	}
	cursor2 := bucketRepresentativeSortValue(t, 8)

	_, page3, _ := collectCollapsed(t, 2, 0, cursor2, false, context.Background())
	if !reflect.DeepEqual(page3, []string{"p3-a"}) {
		t.Fatalf("After 第三页期望 [p3-a]，实际 %v", page3)
	}

	// 同组其他变体(p1-b/p1-c/p2-b)任何一页都不得出现
	for _, ids := range [][]string{page1, page2, page3} {
		for _, id := range ids {
			if id == "p1-b" || id == "p1-c" || id == "p2-b" {
				t.Errorf("非代表变体 %s 泄漏到分页结果中", id)
			}
		}
	}
}

func TestCollapsingCollectorBeforeCursor(t *testing.T) {
	// 以最后一个代表 p3-a 的排序值为 Before 游标，应返回紧邻其前的 2 个代表
	cursorP3 := bucketRepresentativeSortValue(t, 7)
	_, page, _ := collectCollapsed(t, 2, 0, cursorP3, true, context.Background())
	if !reflect.DeepEqual(page, []string{"solo-1", "solo-2"}) {
		t.Fatalf("Before(p3-a) 期望 [solo-1 solo-2]（原始排序输出），实际 %v", page)
	}

	// 继续向前翻页
	cursorSolo1 := bucketRepresentativeSortValue(t, 6)
	_, page, _ = collectCollapsed(t, 2, 0, cursorSolo1, true, context.Background())
	if !reflect.DeepEqual(page, []string{"p1-a", "p2-a"}) {
		t.Fatalf("Before(solo-1) 期望 [p1-a p2-a]，实际 %v", page)
	}
}

// bucketRepresentativeSortValue 通过一次独立采集取得指定编号代表的排序值，
// 模拟调用方从上一页结果中取出游标
func bucketRepresentativeSortValue(t *testing.T, number uint64) [][]byte {
	t.Helper()

	searcher := &collapseStubSearcher{docs: collapseVariantDocs()}
	hc := NewCollapsingTopNCollector(10, 0, collapseSortOrder(), nil, false, "product")
	dmi, err := hc.Collect(context.Background(), collapseAggs(), searcher)
	if err != nil {
		t.Fatalf("获取游标采集失败: %v", err)
	}
	result, err := dmi.Next()
	for result != nil && err == nil {
		if result.Number == number {
			// 返回拷贝，避免与采集器内部内存产生别名
			rv := make([][]byte, len(result.SortValue))
			for i := range result.SortValue {
				rv[i] = append([]byte(nil), result.SortValue[i]...)
			}
			return rv
		}
		result, err = dmi.Next()
	}
	if err != nil {
		t.Fatalf("获取游标遍历失败: %v", err)
	}
	t.Fatalf("未找到编号 %d 的代表文档", number)
	return nil
}

func TestCollapsingCollectorRepeatable(t *testing.T) {
	_, first, _ := collectCollapsed(t, 10, 0, nil, false, context.Background())
	_, second, _ := collectCollapsed(t, 10, 0, nil, false, context.Background())
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("重复搜索结果不一致: %v != %v", first, second)
	}
}

func TestCollapsingCollectorErrorMultiValued(t *testing.T) {
	docs := collapseVariantDocs()
	// 给 p2-a（下标 4）增加第二个 product 值，违反单值 keyword 约定
	docs[4].docVals["product"] = [][]byte{[]byte("P2"), []byte("P2-X")}

	searcher := &collapseStubSearcher{docs: docs}
	hc := NewCollapsingTopNCollector(10, 0, collapseSortOrder(), nil, false, "product")
	_, err := hc.Collect(context.Background(), collapseAggs(), searcher)
	if err == nil {
		t.Fatal("折叠字段多值时应当返回错误")
	}

	// 失败后重新发起一次正常搜索，结果必须正常（无部分分组状态残留）
	_, ids, _ := collectCollapsed(t, 10, 0, nil, false, context.Background())
	expectIDs := []string{"p1-a", "p2-a", "solo-1", "solo-2", "p3-a"}
	if !reflect.DeepEqual(ids, expectIDs) {
		t.Fatalf("失败后的后续搜索结果异常: %v", ids)
	}
}

func TestCollapsingCollectorCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	searcher := &collapseStubSearcher{docs: collapseVariantDocs()}
	hc := NewCollapsingTopNCollector(10, 0, collapseSortOrder(), nil, false, "product")
	if _, err := hc.Collect(ctx, collapseAggs(), searcher); err == nil {
		t.Fatal("已取消的上下文应当导致采集失败")
	}

	// 同一请求参数重新采集，必须得到完整正确结果
	_, ids, bucket := collectCollapsed(t, 10, 0, nil, false, context.Background())
	expectIDs := []string{"p1-a", "p2-a", "solo-1", "solo-2", "p3-a"}
	if !reflect.DeepEqual(ids, expectIDs) {
		t.Fatalf("取消后重试结果异常: %v", ids)
	}
	if total := bucket.Count(); total != 8 {
		t.Errorf("取消后重试期望聚合 count 为 8，实际 %d", total)
	}
}

func TestCollapsingCollectorSizeZero(t *testing.T) {
	searcher := &collapseStubSearcher{docs: collapseVariantDocs()}
	hc := NewCollapsingTopNCollector(0, 0, collapseSortOrder(), nil, false, "product")
	dmi, err := hc.Collect(context.Background(), collapseAggs(), searcher)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dmi.Next()
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatalf("size 为 0 时不应返回命中，实际得到编号 %d", result.Number)
	}
	if total := dmi.Aggregations().Count(); total != 8 {
		t.Errorf("size 为 0 时聚合仍应统计全部匹配，期望 8 实际 %d", total)
	}
}

func TestCollapsingCollectorNoMatches(t *testing.T) {
	searcher := &collapseStubSearcher{docs: nil}
	hc := NewCollapsingTopNCollector(10, 0, collapseSortOrder(), nil, false, "product")
	dmi, err := hc.Collect(context.Background(), collapseAggs(), searcher)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dmi.Next()
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatalf("无匹配时不应返回命中，实际得到编号 %d", result.Number)
	}
	if total := dmi.Aggregations().Count(); total != 0 {
		t.Errorf("无匹配时期望 count 0，实际 %d", total)
	}
}

// 保证采集器实现 search.Collector 接口
var _ search.Collector = &CollapsingTopNCollector{}

func ExampleCollapsingTopNCollector() {
	// 商品变体按分数折叠：同一 product 仅返回排序最靠前的变体
	order := search.SortOrder{
		search.SortBy(search.DocumentScore()).Desc(),
		search.SortBy(search.Field("product")),
	}
	hc := NewCollapsingTopNCollector(10, 0, order, nil, false, "product")
	fmt.Println(hc.BackingSize() > 0)
	// Output: true
}
