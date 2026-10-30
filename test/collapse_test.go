//  Copyright (c) 2020 The Bluge Authors.
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

package test

import (
	"context"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/aggregations"
)

// collapseVariant 表示一个商品变体文档
type collapseVariant struct {
	id      string
	product string // 为空表示该文档没有 product 字段
	rank    float64
}

// collapseLoadData 打乱插入顺序，验证代表是在“全部匹配文档”中
// 全局选出的，而非依赖遍历顺序
func collapseLoadData() []collapseVariant {
	return []collapseVariant{
		{id: "p1-v1", product: "P1", rank: 10},
		{id: "p2-v2", product: "P2", rank: 5},
		{id: "solo-x", product: "", rank: 25},
		{id: "p1-v2", product: "P1", rank: 30},
		{id: "p3-v1", product: "P3", rank: 15},
		{id: "p2-v1", product: "P2", rank: 40},
		{id: "p1-v3", product: "P1", rank: 20},
		{id: "solo-y", product: "", rank: 8},
	}
}

// 按 rank 降序折叠后的代表序列：
// p2-v1(40)、p1-v2(30)、solo-x(25)、p3-v1(15)、solo-y(8)
var collapseExpectedRepresentatives = []string{
	"p2-v1", "p1-v2", "solo-x", "p3-v1", "solo-y",
}

// collapseInsert 写入一个变体文档
func collapseInsert(writer *bluge.Writer, v collapseVariant) error {
	doc := bluge.NewDocument(v.id).
		AddField(bluge.NewNumericField("rank", v.rank))
	if v.product != "" {
		doc.AddField(bluge.NewKeywordField("product", v.product).Sortable())
	}
	return writer.Insert(doc)
}

func openCollapseIndex(t *testing.T) (*bluge.Reader, func()) {
	t.Helper()

	path, err := ioutil.TempDir("", "bluge-collapse-test")
	if err != nil {
		t.Fatal(err)
	}
	cfg := bluge.DefaultConfig(path)
	idx, err := bluge.OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range collapseLoadData() {
		if err := collapseInsert(idx, v); err != nil {
			t.Fatalf("写入变体 %s 失败: %v", v.id, err)
		}
	}
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	return reader, func() {
		_ = reader.Close()
		_ = idx.Close()
	}
}

func collapseRequest(size int) *bluge.TopNSearch {
	return bluge.NewTopNSearch(size, bluge.NewMatchAllQuery()).
		SortBy([]string{"-rank", "_id"}).
		CollapseBy("product")
}

func collapseSearch(t *testing.T, reader *bluge.Reader, req *bluge.TopNSearch,
	ctx context.Context) ([]string, uint64) {
	t.Helper()

	req.AddAggregation("count", aggregations.CountMatches())
	dmi, err := reader.Search(ctx, req)
	if err != nil {
		t.Fatalf("执行折叠搜索失败: %v", err)
	}
	hits, err := collectHits(dmi)
	if err != nil {
		t.Fatalf("收集结果失败: %v", err)
	}

	ids := make([]string, len(hits))
	for i, hit := range hits {
		ids[i] = string(hit.Fields["_id"][0])
	}
	return ids, dmi.Aggregations().Count()
}

func copySortValue(in [][]byte) [][]byte {
	rv := make([][]byte, len(in))
	for i := range in {
		rv[i] = append([]byte(nil), in[i]...)
	}
	return rv
}

func TestCollapseByProductVariants(t *testing.T) {
	reader, cleanup := openCollapseIndex(t)
	defer cleanup()

	aggs := search.Aggregations{
		"count": aggregations.CountMatches(),
	}

	// 1) 不折叠时，全部 8 个变体都返回
	plainReq := bluge.NewTopNSearch(20, bluge.NewMatchAllQuery()).
		SortBy([]string{"-rank", "_id"})
	plainReq.AddAggregation("count", aggregations.CountMatches())
	plainDmi, err := reader.Search(context.Background(), plainReq)
	if err != nil {
		t.Fatal(err)
	}
	plainHits, err := collectHits(plainDmi)
	if err != nil {
		t.Fatal(err)
	}
	if len(plainHits) != 8 {
		t.Fatalf("未折叠时应返回全部 8 个变体，实际 %d", len(plainHits))
	}
	if total := plainDmi.Aggregations().Count(); total != 8 {
		t.Errorf("未折叠时期望 count=8，实际 %d", total)
	}

	// 2) SetFrom 按商品（组）翻页，每页 2 个代表
	var fromSequence []string
	pageSizes := map[int]int{}
	for from := 0; from < 6; from += 2 {
		req := collapseRequest(2).SetFrom(from)
		for name, agg := range aggs {
			req.AddAggregation(name, agg)
		}
		ids, total := collapseSearch(t, reader, req, context.Background())
		pageSizes[from] = len(ids)
		// 聚合始终统计全部 8 个原始匹配文档
		if total != 8 {
			t.Errorf("from=%d 时期望聚合 count=8（原始变体数），实际 %d", from, total)
		}
		fromSequence = append(fromSequence, ids...)
	}
	if !reflect.DeepEqual(pageSizes, map[int]int{0: 2, 2: 2, 4: 1}) {
		t.Fatalf("期望各页大小 map[0:2 2:2 4:1]，实际 %v", pageSizes)
	}
	if !reflect.DeepEqual(fromSequence, collapseExpectedRepresentatives) {
		t.Fatalf("SetFrom 翻页序列期望 %v，实际 %v",
			collapseExpectedRepresentatives, fromSequence)
	}

	// 3) After 游标按组翻页，序列必须与 SetFrom 完全一致，
	//    且非代表变体永不出现在任何页
	var afterSequence []string
	var cursor [][]byte
	for {
		req := collapseRequest(2)
		if cursor != nil {
			req.After(cursor)
		}
		ids, sortValues, total := collapseSearchWithSortValues(t, reader, req, context.Background())
		if total != 8 {
			t.Errorf("After 翻页期望聚合 count=8，实际 %d", total)
		}
		if len(ids) == 0 {
			break
		}
		afterSequence = append(afterSequence, ids...)
		cursor = copySortValue(sortValues[len(sortValues)-1])
	}
	if !reflect.DeepEqual(afterSequence, collapseExpectedRepresentatives) {
		t.Fatalf("After 翻页序列期望 %v，实际 %v",
			collapseExpectedRepresentatives, afterSequence)
	}
	assertOnlyRepresentatives(t, afterSequence)

	// 4) Before 游标反向翻页：从最后一个代表之前开始逐页回退，
	//    同样不得出现非代表变体或跨页重复的商品
	var beforeSeen [][]string
	cursor = beforeCursorOf(t, reader, "solo-y")
	for cursor != nil {
		req := collapseRequest(2).Before(cursor)
		ids, sortValues, _ := collapseSearchWithSortValues(t, reader, req, context.Background())
		if len(ids) == 0 {
			break
		}
		beforeSeen = append(beforeSeen, ids)
		assertOnlyRepresentatives(t, ids)
		// 下一页以本页第一个代表（原始排序最靠前）的排序值为界继续向前
		cursor = copySortValue(sortValues[0])
	}
	// 回退结果顺序拼起来应为 [solo-x p3-v1] [p2-v1 p1-v2]
	expectBeforePages := [][]string{
		{"solo-x", "p3-v1"},
		{"p2-v1", "p1-v2"},
	}
	if !reflect.DeepEqual(beforeSeen, expectBeforePages) {
		t.Fatalf("Before 翻页期望 %v，实际 %v", expectBeforePages, beforeSeen)
	}

	// 5) 相邻页不得出现同一商品的其他变体：正向每页内商品唯一，
	//    且所有页商品互不重复
	productsSeen := make(map[string]struct{})
	for _, id := range afterSequence {
		product := productOfVariant(id)
		if _, ok := productsSeen[product]; ok && product != "" {
			t.Errorf("商品 %s 在翻页结果中重复出现（命中 %s）", product, id)
		}
		productsSeen[product] = struct{}{}
	}

	// 6) 重复执行同一请求，结果必须稳定一致
	idsFirst, _, _ := collapseSearchWithSortValues(t, reader, collapseRequest(10), context.Background())
	idsSecond, _, _ := collapseSearchWithSortValues(t, reader, collapseRequest(10), context.Background())
	if !reflect.DeepEqual(idsFirst, idsSecond) {
		t.Fatalf("重复折叠搜索结果不一致: %v != %v", idsFirst, idsSecond)
	}
}

func TestCollapseByFailureLeavesNoState(t *testing.T) {
	reader, cleanup := openCollapseIndex(t)
	defer cleanup()

	// 复用同一个请求实例：先用已取消的上下文制造中途失败
	req := collapseRequest(10)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.Search(canceled, req); err == nil {
		t.Fatal("已取消的上下文应当使搜索失败")
	}

	// 同一请求立即重试，必须得到完整正确的折叠结果，且聚合计数不变
	ids, sortValues, total := collapseSearchWithSortValues(t, reader, req, context.Background())
	if !reflect.DeepEqual(ids, collapseExpectedRepresentatives) {
		t.Fatalf("失败后重试结果期望 %v，实际 %v", collapseExpectedRepresentatives, ids)
	}
	if total != 8 {
		t.Errorf("失败后期望聚合 count=8，实际 %d", total)
	}

	// 再用末页代表游标翻最后一页，验证失败后游标机制未受污染
	lastReq := collapseRequest(10).After(copySortValue(sortValues[len(sortValues)-2]))
	lastIDs, _, _ := collapseSearchWithSortValues(t, reader, lastReq, context.Background())
	if !reflect.DeepEqual(lastIDs, []string{"solo-y"}) {
		t.Fatalf("失败后 After 翻页期望 [solo-y]，实际 %v", lastIDs)
	}
}

func TestCollapseByMultiValuedFieldErrors(t *testing.T) {
	path, err := ioutil.TempDir("", "bluge-collapse-multivalue")
	if err != nil {
		t.Fatal(err)
	}
	idx, err := bluge.OpenWriter(bluge.DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idx.Close() }()

	// 同一文档写入两个同名 keyword 字段，违反单值约定
	err = idx.Insert(bluge.NewDocument("mv-1").
		AddField(bluge.NewNumericField("rank", 1.0)).
		AddField(bluge.NewKeywordField("product", "P1").Sortable()).
		AddField(bluge.NewKeywordField("product", "P2").Sortable()))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()

	req := bluge.NewTopNSearch(10, bluge.NewMatchAllQuery()).
		SortBy([]string{"-rank", "_id"}).
		CollapseBy("product")
	if _, err := reader.Search(context.Background(), req); err == nil {
		t.Fatal("折叠字段在匹配文档上为多值时应当返回错误")
	}
}

// collapseSearchWithSortValues 在返回 id 的同时保留每条命中的排序值，
// 供 After/Before 游标使用
func collapseSearchWithSortValues(t *testing.T, reader *bluge.Reader,
	req *bluge.TopNSearch, ctx context.Context) ([]string, [][][]byte, uint64) {
	t.Helper()

	req.AddAggregation("count", aggregations.CountMatches())
	dmi, err := reader.Search(ctx, req)
	if err != nil {
		t.Fatalf("执行折叠搜索失败: %v", err)
	}

	var ids []string
	var sortValues [][][]byte
	next, err := dmi.Next()
	for next != nil && err == nil {
		var id []byte
		if err := next.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_id" {
				id = append([]byte(nil), value...)
				return false
			}
			return true
		}); err != nil {
			t.Fatalf("读取 _id 失败: %v", err)
		}
		ids = append(ids, string(id))
		sortValues = append(sortValues, copySortValue(next.SortValue))
		next, err = dmi.Next()
	}
	if err != nil {
		t.Fatalf("遍历折叠结果失败: %v", err)
	}
	return ids, sortValues, dmi.Aggregations().Count()
}

// beforeCursorOf 取得指定代表当前的排序值，作为 Before 游标
func beforeCursorOf(t *testing.T, reader *bluge.Reader, id string) [][]byte {
	t.Helper()

	ids, sortValues, _ := collapseSearchWithSortValues(t, reader, collapseRequest(10),
		context.Background())
	for i := range ids {
		if ids[i] == id {
			return sortValues[i]
		}
	}
	t.Fatalf("未找到代表 %s 的排序值", id)
	return nil
}

// assertOnlyRepresentatives 断言页面中只允许出现组代表
func assertOnlyRepresentatives(t *testing.T, ids []string) {
	t.Helper()

	allowed := map[string]struct{}{}
	for _, id := range collapseExpectedRepresentatives {
		allowed[id] = struct{}{}
	}
	for _, id := range ids {
		if _, ok := allowed[id]; !ok {
			t.Errorf("非代表变体 %s 不允许出现在折叠结果中", id)
		}
	}
}

// productOfVariant 依据测试数据命名返回变体所属商品
func productOfVariant(id string) string {
	for _, v := range collapseLoadData() {
		if v.id == id {
			return v.product
		}
	}
	return ""
}
