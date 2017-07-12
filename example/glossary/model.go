package glossary

type GlossDefModel struct {
	SK           int64 `db:"pk"`
	Para         string
	GlossSeeAlso string
}

type GlossEntryModel struct {
	SK         int64 `db:"pk"`
	ID         string
	SortAs     string
	GlossTerm  string
	Acronym    string
	Abbrev     string
	GlossSee   string
	GlossDefSK int64
}

type GlossListModel struct {
	SK           int64 `db:"pk"`
	GlossEntrySK int64
}

type GlossDivModel struct {
	SK          int64 `db:"pk"`
	Title       string
	GlossListSK int64
}

type GlossaryModel struct {
	SK         int64 `db:"pk"`
	Title      string
	GlossDivSK int64
}

type DataModel struct {
	SK         int64 `db:"pk"`
	GlossarySK int64
}
