package filter

import paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"

var (
	queryStringConverter  = NewQueryStringConverter()
	filterStringConverter = NewFilterStringConverter()
)

type filterRequester interface {
	GetFilterExpr() *paginationV1.FilterExpr
	GetQuery() string
	GetFilter() string
}

// convertFilterRequest converts a filterRequester to a FilterExpr.
func convertFilterRequest(req filterRequester) (*paginationV1.FilterExpr, error) {
	if req == nil {
		return nil, nil
	}

	if req.GetFilterExpr() != nil {
		return req.GetFilterExpr(), nil
	}

	if q := req.GetQuery(); q != "" {
		return queryStringConverter.Convert(q)
	}

	if f := req.GetFilter(); f != "" {
		return filterStringConverter.Convert(f)
	}

	return nil, nil
}

// ConvertFilterByPagingRequest converts a PagingRequest to a FilterExpr.
func ConvertFilterByPagingRequest(req *paginationV1.PagingRequest) (*paginationV1.FilterExpr, error) {
	return convertFilterRequest(req)
}

// ConvertFilterByPaginationRequest converts a PaginationRequest to a FilterExpr.
func ConvertFilterByPaginationRequest(req *paginationV1.PaginationRequest) (*paginationV1.FilterExpr, error) {
	return convertFilterRequest(req)
}
