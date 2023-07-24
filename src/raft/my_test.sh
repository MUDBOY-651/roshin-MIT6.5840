#!/bin/bash
# 设置测试次数
num_tests=200
# 设置并发进程数
num_processes=8
# 统计通过的测试次数
pass_count=0

# 创建一个临时文件用于保存测试结果
result_file=$(mktemp)

# 定义一个函数，用于执行单个测试并判断是否通过
run_test() {
    output=$(go test --run 2C 2>&1)
    if echo "$output" | grep -q "PASS"; then
        echo "Test passed." >> "$result_file"
    else
        echo "Test failed." >> "$result_file"
    fi
}

# 设置trap，确保在脚本结束时删除临时文件
cleanup() {
    rm -f "$result_file"
}
trap cleanup EXIT

# 运行多个测试并记录结果
for ((i=0; i<num_tests; i++)); do
    run_test &
    # 控制并发进程数
    if (( (i+1) % num_processes == 0 )); then
        wait
    fi
done

# 等待所有测试完成
wait

# 统计通过的测试次数
pass_count=$(grep -c "Test passed" "$result_file")

# 输出结果
echo "Total tests: $num_tests"
echo "Tests passed: $pass_count"

