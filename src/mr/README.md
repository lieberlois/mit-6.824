Run coordinator

```bash
go run -race mrcoordinator.go pg*.txt
```

Run Worker

```bash
# Build mapreduce job as a shared object
go build -race -buildmode=plugin ../mrapps/wc.go

# Run worker 
go run -race mrworker.go wc.so
```