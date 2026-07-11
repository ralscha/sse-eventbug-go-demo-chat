# sse-eventbug-go-demo-chat

Go backend for the Ionic/Angular chat application from
[`sse-eventbus-demo-chat`](https://github.com/ralscha/sse-eventbus-demo-chat).
The client is unchanged and the backend uses `sse-eventbus-go` for room and
global SSE topics.

Run the backend and client in separate terminals:

```text
task server
task client
```

Open `http://localhost:4200`. The client talks to the Go backend on port 8080.

## License

MIT License. See [LICENSE](LICENSE) for details.
