package consumer

import "github.com/pkg/errors"

type MessageErrorType int

const (
	// PanicErrorType Ошибка, которую не исправить повторением через промежуток времени
	PanicErrorType MessageErrorType = iota
	// CanTryToFixErrorType Ошибка, которую можно попробовать исправить путем повторения
	CanTryToFixErrorType
	// InfoErrorType Ошибка для информирования
	InfoErrorType
)

type ProcessedMsgError struct {
	Type MessageErrorType
	Err  error
}

func (r *ProcessedMsgError) Error() string {
	if r == nil || r.Err == nil {
		return ""
	}
	return r.Err.Error()
}

func MakePanicErr(err error) error {
	return &ProcessedMsgError{Err: err, Type: PanicErrorType}
}

func MakePanicWrapErr(err error, msg string) error {
	return &ProcessedMsgError{Err: errors.Wrap(err, msg), Type: PanicErrorType}
}

func MakeCanTryFixErr(err error) error {
	return &ProcessedMsgError{Err: err, Type: CanTryToFixErrorType}
}

func MakeCanTryFixWrapErr(err error, msg string) error {
	return &ProcessedMsgError{Err: errors.Wrap(err, msg), Type: CanTryToFixErrorType}
}

func MakeInfoErrorTypeErr(err error) error {
	return &ProcessedMsgError{Err: err, Type: InfoErrorType}
}

func MakeInfoErrorTypeWrapErr(err error, msg string) error {
	return &ProcessedMsgError{Err: errors.Wrap(err, msg), Type: InfoErrorType}
}
