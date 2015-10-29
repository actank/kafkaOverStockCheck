#!/bin/bash
source /home/work/.bashrc

cluster=$1
ZROOTNODE=$2
TOPIC=$3
PARTITIONNUM=$4
CALLBACKURL=$5
if [ ! -n "$cluster" ] || [ ! -n "$ZROOTNODE" ] || [ ! -n "$TOPIC" ] || [ ! -n "$PARTITIONNUM" ]  || [ ! -n "$CALLBACKURL"
then
        echo "usage ./distance.sh clustername zrootnode{cart, order, coupon, promotion, goods} topic partitionnum callback"
    echo 'example: sh distance.sh kafka-order-dfz order order_op 64 http://meilipush.meilishuo.com/transmit/ReceiveDootaPu'
        exit
fi


out=`fsh "10.0.20.40" "cat /home/work/opdir/newgroup/$cluster"`
i=0
while read _line
do
        HOSTS[$i]=$_line
        let i++
done << EOF
$out
EOF

CG=`echo $CALLBACKURL | tr -d '\n' | md5sum | cut -d ' ' -f1`

filename="/home/work/kafka-$ZROOTNODE/logs/replication-offset-checkpoint"
filename2="/data/kafka-$ZROOTNODE/logs/replication-offset-checkpoint"

for host in ${HOSTS[@]}
do
        echo ${host}
    OUT=`fsh "${host}" "if [ -f $filename ] ;then cat $filename | grep $TOPIC | sort -k2 -n | uniq;else cat $filename2 | grep '$TOPIC' | sort -k2 -n | uniq;fi;"`
        while read _topic _partition _offset
        do
                data[$_partition]=$_offset
        done << EOF
$OUT
EOF
done

sum=0
z sh "${HOSTS[0]}:2181/kafka/$ZROOTNODE"
for x in `seq 0 $PARTITIONNUM`
do
    sleep 0.05
    consumeOffset=`z get consumers/$CG/offsets/$TOPIC/$x`
    produceOffset=${data[$x]}
    if [[ $produceOffset -gt "0" ]] && [[ $consumeOffset -gt "0" ]]
    then
    distance=$[$produceOffset-$consumeOffset]
        sum=$[$sum+$distance]
    echo "topic:" $TOPIC " partition:" $x " " consumeOffset:" $consumeOffset " produceOffset:" $produceOffset distance:" $distance
    fi
done
echo "total distance:"$sum
