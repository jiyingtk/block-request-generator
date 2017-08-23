method_name=('raid-5' 'oi-raid' 'rs69' 's2-raid' 'parity-declustering')
run_id=(0 1 2)
request_type=('offline' 'online')
running_time=600
#trace_name=('WebSearch1.spc' 'Financial1.spc')
#trace_name=('WebSearch1.spc')
trace_name=('Financial1.spc')

for i in $(seq 0 `expr ${#run_id[@]} - 1`)
do
    for j in $(seq 0 `expr ${#trace_name[@]} - 1`)
    do 
        if [ $j -eq 0 ];then
            out_folder='result-degraded-Web-s2-raid-fin'
        elif [ $j -eq 1 ];then
            out_folder='result-normal-Fin'
        fi
        if [ ! -e $out_folder ];then
            mkdir $out_folder
        fi


        for ((method=0;method<=4;method++))
        do
            if [ ! $method -eq 3 ];then
                continue
            fi
            #echo ${method_name[$method]} ${request_type[$i]} disknum 21 capacity 100G >> running-time.txt
            ../request $method 7 3 3 4096 1024 $running_time devices.txt ../../trace/${trace_name[$j]} $out_folder/${method_name[$method]}-21.txt${run_id[$i]}

            #echo ${method_name[$method]} ${request_type[$i]} disknum 27 capacity 100G >> running-time.txt
            #../request $method 9 3 3 4096 1024 $running_time devices.txt ../../trace/${trace_name[$j]} $out_folder/${method_name[$method]}-27.txt${run_id[$i]}

            #echo ${method_name[$method]} ${request_type[$i]} disknum 45 capacity 100G >> running-time.txt
            ../request $method 15 3 3 4096 1024 $running_time devices.txt ../../trace/${trace_name[$j]} $out_folder/${method_name[$method]}-45.txt${run_id[$i]} 

            #echo ${method_name[$method]} ${request_type[$i]} disknum 57 capacity 100G >> running-time.txt
            #../request $method 19 3 3 4096 1024 $running_time devices.txt ../../trace/${trace_name[$j]} $out_folder/${method_name[$method]}-57.txt${run_id[$i]} 
        done
    done
done

