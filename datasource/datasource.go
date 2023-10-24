package datasource

// MySQL type information.
const (
	TypeUnspecified     uint16 = 0
	TypeTikv            uint16 = 1
	TypeMySQL           uint16 = 10 //
	TypeMySQL57         uint16 = 11 //
	TypeMySQL80         uint16 = 12 //
	TypeMariaDB         uint16 = 20 //
	TypeGoldenDB        uint16 = 30 //
	TypeGoldenDBSingle  uint16 = 31 //
	TypeGoldenDBCluster uint16 = 40 //

	TypeIbmDB         uint16 = 50 //
	TypeHbase         uint16 = 60 //
	TypeElasticSearch uint16 = 70 //

	TypeMax uint16 = 0xffff
)

func GetDataSourceType(name string) uint16 {
	switch name {
	case "hbase":
		return TypeHbase
	case "tikv":
		return TypeTikv
	default:
		return TypeTikv
	}
}
