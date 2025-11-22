"""Thin wrapper that runs the shared kptn_server JSON-RPC backend for VS Code."""

from kptn_server.api_jsonrpc import main


if __name__ == "__main__":
	main()

