# distributed-system

test command:
```
cat test_test.go | grep -E '^func Test' | sed 's/func \([^ ]*\)(.*/\1/' | xargs ../../dstest.py -p 4 -o .run -v 1 -r -s -n 10
```
