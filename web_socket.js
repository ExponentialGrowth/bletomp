function connect() {
    const url = 'wss://plumular-unwrathful-kristan.ngrok-free.dev/ws';
    const socket = new WebSocket(url);

    socket.onopen = () => {
        console.log('%c✅ CONNECTED to ngrok tunnel', 'color: green; font-weight: bold');
        socket.send(JSON.stringify({ message: "Hello from the test script!" }));
    };

    socket.onmessage = (event) => {
        console.log('%c📩 RECEIVED:', 'color: blue', event.data);
    };

    socket.onclose = (e) => {
        console.log('%c🔌 DISCONNECTED. Retrying in 3s...', 'color: orange');
        setTimeout(connect, 3000); // Try to reconnect
    };

    socket.onerror = (err) => {
        console.error('❌ WebSocket Error:', err);
        socket.close();
    };
}

connect();