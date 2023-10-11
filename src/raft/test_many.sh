for i in {1..1000};
do
    echo "这是第 $i 次测试"
    go test -run 2B
done