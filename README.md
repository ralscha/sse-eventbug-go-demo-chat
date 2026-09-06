# sse-eventbug-go-demo-chat

Go backend for the Ionic/Angular chat application from
[`sse-eventbus-demo-chat`](https://github.com/ralscha/sse-eventbus-demo-chat).
The backend uses `sse-eventbus-go` for room and global SSE topics. Heartbeats
keep active sessions alive, while disconnected sessions expire after one hour.

Run the backend and client in separate terminals:

```text
task server
task client
```

Open `http://localhost:4200`. The client talks to the Go backend on port 8080.
Run `task test` for backend tests or `task build` to build both applications.

## License

MIT License. See [LICENSE](LICENSE) for details.
