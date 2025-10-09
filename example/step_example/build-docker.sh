cd ../..
uv build --wheel
cp dist/kapten-*.whl example/step_example/
cd example/step_example
docker build -t kaptenstep .
rm kapten-*.whl