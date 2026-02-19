rustup toolchain uninstall stable-x86_64-unknown-linux-gnu
rustup toolchain install stable --profile default
rustup default stable
cargo clean
cargo +stable check
