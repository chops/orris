// Pinned framework ESM assets copied from the locked dependencies (bin/c1-assets); no CDN, no bundler, no inline script.
import { Socket } from "./phoenix.mjs";
import { LiveSocket } from "./phoenix_live_view.esm.js";

const csrfToken = document.querySelector("meta[name='csrf-token']")?.getAttribute("content");
const mount = document.querySelector("meta[name='orris-console-socket']")?.getAttribute("content") || "/live/websocket";
const socketPath = mount.replace(/\/websocket$/, "");
const liveSocket = new LiveSocket(socketPath, Socket, { params: { _csrf_token: csrfToken }, longPollFallbackMs: undefined });
liveSocket.connect();
window.liveSocket = liveSocket;
