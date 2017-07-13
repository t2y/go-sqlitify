package sqlitify

import (
	uuid "github.com/nu7hatch/gouuid"
)

func GetUUID() string {
	_uuid, _ := uuid.NewV4()
	return _uuid.String()
}
