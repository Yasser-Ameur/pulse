# Records a terminal walkthrough of Pulse: start the broker, create a topic,
# publish, and watch a live subscribe pick the records up.
#
# Requires linux/amd64 pulse-server and pulse-cli binaries in the same
# directory as this tape (built with `go build ./cmd/pulse-server` /
# `./cmd/pulse-cli` inside golang:1.26, CGO_ENABLED=0), and pulse.yaml
# beside them. Run with:
#   docker run --rm -v "<dir with this tape and the binaries>:/work" -w /work \
#     ghcr.io/charmbracelet/vhs:latest demo.tape

Output demo.gif

Set Width 1200
Set Height 600
Set FontSize 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 45ms
Set Padding 20
Set Shell bash

Type "./pulse-server --config pulse.yaml > server.log 2>&1 &"
Enter
Sleep 1500ms

Type "./pulse-cli topics create shipment-events"
Enter
Sleep 1500ms

Type "./pulse-cli subscribe shipment-events --follow --consumer fulfillment-worker &"
Enter
Sleep 1000ms

Type `./pulse-cli publish shipment-events --key order-4471 --message '{"status":"packed"}'`
Enter
Sleep 1800ms

Type `./pulse-cli publish shipment-events --key order-4471 --message '{"status":"shipped"}'`
Enter
Sleep 1800ms

Type `./pulse-cli publish shipment-events --key order-8823 --message '{"status":"packed"}'`
Enter
Sleep 3000ms
