package impl

import (
	"nebula-tracker/db"
	chooser "nebula-tracker/metadata/provider_chooser"
)

type providerChooser interface {
	Count() int
	Choose(num uint32) []db.ProviderInfo
}

type chooserImpl struct {
}

func (self *chooserImpl) Count() int {
	return chooser.Count()
}

func (self *chooserImpl) Choose(num uint32) []db.ProviderInfo {
	return chooser.Choose(num)
}
