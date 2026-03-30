package displaytime

import (
	"reflect"
	"sync"
	"time"

	_ "time/tzdata"
)

const KSTName = "Asia/Seoul"

var (
	kstOnce sync.Once
	kstLoc  *time.Location
)

func KST() *time.Location {
	kstOnce.Do(func() {
		location, err := time.LoadLocation(KSTName)
		if err != nil {
			location = time.FixedZone("KST", 9*60*60)
		}
		kstLoc = location
	})
	return kstLoc
}

func UseKSTAsLocal() {
	time.Local = KST()
}

func InKST(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	return value.In(KST())
}

func NormalizeForJSON(value any) any {
	if value == nil {
		return nil
	}
	return normalizeReflect(reflect.ValueOf(value)).Interface()
}

var timeType = reflect.TypeOf(time.Time{})

func normalizeReflect(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}

	if value.Type() == timeType {
		return reflect.ValueOf(InKST(value.Interface().(time.Time)))
	}

	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		normalizedElem := normalizeReflect(value.Elem())
		ptr := reflect.New(value.Type().Elem())
		ptr.Elem().Set(normalizedElem)
		return ptr
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		normalizedElem := normalizeReflect(value.Elem())
		wrapped := reflect.New(value.Elem().Type()).Elem()
		wrapped.Set(normalizedElem)
		return wrapped
	case reflect.Struct:
		clone := reflect.New(value.Type()).Elem()
		clone.Set(value)
		for index := 0; index < value.NumField(); index++ {
			field := clone.Field(index)
			if !field.CanSet() {
				continue
			}
			field.Set(normalizeReflect(value.Field(index)))
		}
		return clone
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			clone.Index(index).Set(normalizeReflect(value.Index(index)))
		}
		return clone
	case reflect.Array:
		clone := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			clone.Index(index).Set(normalizeReflect(value.Index(index)))
		}
		return clone
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			clone.SetMapIndex(iterator.Key(), normalizeReflect(iterator.Value()))
		}
		return clone
	default:
		return value
	}
}
