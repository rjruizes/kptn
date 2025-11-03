cd ../..
uv build --wheel
cp dist/kptn-*.whl example/step_example/
cd example/step_example
docker build -t kptnstep .
# rm kptn-*.whl