package query

import optionsV2 "go.mongodb.org/mongo-driver/v2/mongo/options"

type findOptsLister struct {
	opts *optionsV2.FindOptions
}

func (l *findOptsLister) List() []func(*optionsV2.FindOptions) error {
	var list []func(*optionsV2.FindOptions) error
	if l == nil || l.opts == nil {
		return list
	}
	if l.opts.Limit != nil {
		lim := *l.opts.Limit
		list = append(list, func(o *optionsV2.FindOptions) error {
			o.Limit = &lim
			return nil
		})
	}
	if l.opts.Skip != nil {
		sk := *l.opts.Skip
		list = append(list, func(o *optionsV2.FindOptions) error {
			o.Skip = &sk
			return nil
		})
	}
	if l.opts.Sort != nil {
		sortVal := l.opts.Sort
		list = append(list, func(o *optionsV2.FindOptions) error {
			o.Sort = sortVal
			return nil
		})
	}
	if l.opts.Projection != nil {
		proj := l.opts.Projection
		list = append(list, func(o *optionsV2.FindOptions) error {
			o.Projection = proj
			return nil
		})
	}
	// 可根据需要继续加入其它字段（Collation、Hint、BatchSize 等）
	return list
}

type findOneOptsLister struct {
	opts *optionsV2.FindOneOptions
}

func (l *findOneOptsLister) List() []func(*optionsV2.FindOneOptions) error {
	var list []func(*optionsV2.FindOneOptions) error
	if l == nil || l.opts == nil {
		return list
	}
	if l.opts.Skip != nil {
		sk := *l.opts.Skip
		list = append(list, func(o *optionsV2.FindOneOptions) error {
			o.Skip = &sk
			return nil
		})
	}
	if l.opts.Sort != nil {
		sortVal := l.opts.Sort
		list = append(list, func(o *optionsV2.FindOneOptions) error {
			o.Sort = sortVal
			return nil
		})
	}
	if l.opts.Projection != nil {
		proj := l.opts.Projection
		list = append(list, func(o *optionsV2.FindOneOptions) error {
			o.Projection = proj
			return nil
		})
	}
	// 可根据需要继续加入其它字段
	return list
}
