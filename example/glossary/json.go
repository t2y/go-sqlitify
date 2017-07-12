package glossary

type GlossDef struct {
	Para         string   `json:"para"`
	GlossSeeAlso []string `json:"GlossSeeAlso"`
}

type GlossEntry struct {
	ID        string   `json:"ID"`
	SortAs    string   `json:"SortAs"`
	GlossTerm string   `json:"GlossTerm"`
	Acronym   string   `json:"Acronym"`
	Abbrev    string   `json:"Abbrev"`
	GlossSee  string   `json:"GlossSee"`
	GlossDef  GlossDef `json:"GlossDef"`
}

type GlossList struct {
	GlossEntry GlossEntry `json:"GlossEntry"`
}

type GlossDiv struct {
	Title     string    `json:"title"`
	GlossList GlossList `json:"GlossList"`
}

type Glossary struct {
	Title    string   `json:"title"`
	GlossDiv GlossDiv `json:"GlossDiv"`
}

type Data struct {
	Glossary Glossary `json:"glossary"`
}
