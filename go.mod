module github.com/jabolina/go-mcast

go 1.14

require (
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/axw/gocov v1.0.0 // indirect
	github.com/fatih/color v1.9.0 // indirect
	github.com/hashicorp/go-version v1.2.0 // indirect
	github.com/jabolina/relt v0.0.9
	github.com/matm/gocov-html v0.0.0-20200509184451-71874e2e203b // indirect
	github.com/mattn/go-colorable v0.1.6 // indirect
	github.com/mitchellh/gox v1.0.1 // indirect
	github.com/prometheus/common v0.15.0
	github.com/sirupsen/logrus v1.6.0 // indirect
	go.uber.org/goleak v1.0.0
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	gopkg.in/alecthomas/kingpin.v2 v2.2.6 // indirect
)

replace (
	github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5
	github.com/jabolina/relt => /home/josebolina/master/relt
	google.golang.org/grpc v1.35.0 => google.golang.org/grpc v1.26.0
)
