for i in 1 2 3 4 5 ; do
  for j in 1 2 3 4 5; do
    mkdir -p test/$i/$j
    touch test/$i/$j/{aa,bb,cc,dd,ee}.json
    touch test/${i}${j}{aa,bb,cc,dd,ee}.json
  done
done