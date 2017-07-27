
#define BLOCK 4096

struct addr_info {
    int method;
    int disk_nums;
    int failedDisk;
    int strip_size;
    addr_type capacity, capacity_total;

    addr_type blocks_partition;
    addr_type strips_partition;
    addr_type data_blocks;
    int blocks_per_strip;
    int stripe_nums;

    int **bibd, * *spd;
    int b, v, r, k, lambda;
    int g;

    int running_time;
    char *trace_fn;
};

int requestPerSecond = 0;

int **diskArray;           //子阵列对应的 磁盘号
int **offsetArray;         //子阵列对应的 偏移量（分区号）
int **diskRegion;           //每个disk包含的Region号

void makeSubRAID(struct addr_info *ainfo);


void init_parameters(struct addr_info *ainfo) {
    ainfo->disk_nums = ainfo->v * ainfo->g;
    ainfo->blocks_per_strip = ainfo->strip_size / BLOCK;
    ainfo->stripe_nums = ainfo->v * ainfo->g * ainfo->r * (ainfo->g - 1) / ainfo->k;

    ainfo->capacity /= ainfo->strip_size;

    addr_type spareSize = (ainfo->capacity + ainfo->disk_nums - 1) / ainfo->disk_nums;
    ainfo->strips_partition = (ainfo->capacity - spareSize) / (ainfo->g * ainfo->r);
    ainfo->blocks_partition = ainfo->strips_partition * ainfo->blocks_per_strip;

    ainfo->data_blocks = ainfo->capacity - spareSize;
    ainfo->data_blocks *= ainfo->blocks_per_strip;

    spareSize *= ainfo->strip_size;
    ainfo->capacity *= ainfo->strip_size;

    fprintf(stderr, "spareSize %fGB, capacity %fGB\n", spareSize * 1.0f / 1024 / 1024 / 1024, ainfo->capacity * 1.0f / 1024 / 1024 / 1024);

    if (ainfo->method == 0) {   //RAID5
        ainfo->disk_nums = ainfo->disk_nums / ainfo->k * ainfo->k;
        ainfo->capacity_total = ainfo->capacity / BLOCK * ainfo->disk_nums / ainfo->k * (ainfo->k - 1);

    } else {
        ainfo->capacity_total = ainfo->stripe_nums * (ainfo->k - 1) * ainfo->blocks_partition;
    }

}

void init_addr_info(struct addr_info *ainfo) {
    char fn[128];
    sprintf(fn, "%d_%d.bibd", ainfo->v, ainfo->k);
    FILE *bibd_f = fopen(fn, "r");

    fscanf(bibd_f, "%d %d %d %d %d", &ainfo->b, &ainfo->v, &ainfo->r, &ainfo->k, &ainfo->lambda);

    init_parameters(ainfo);

    int i, j;
    int stripe_nums = ainfo->stripe_nums;

    diskArray = (typeof(diskArray)) malloc(sizeof(typeof(*diskArray)) * stripe_nums);
    offsetArray = (typeof(offsetArray)) malloc(sizeof(typeof(*offsetArray)) * stripe_nums);

    for (i = 0; i < stripe_nums; i++) {
        diskArray[i] = (typeof(*diskArray)) malloc(sizeof(typeof(**diskArray)) * ainfo->k);
        offsetArray[i] = (typeof(*offsetArray)) malloc(sizeof(typeof(**offsetArray)) * ainfo->k);
    }

    diskRegion = (typeof(diskRegion)) malloc(sizeof(typeof(*diskRegion)) * ainfo->v * ainfo->g);

    for (i = 0; i < ainfo->v * ainfo->g; i++) {
        diskRegion[i] = (typeof(*diskRegion)) malloc(sizeof(typeof(**diskRegion)) * ainfo->g * ainfo->r);
    }

    int **bibd, **spd;
    bibd = (typeof(bibd)) malloc(sizeof(typeof(*bibd)) * ainfo->b);

    for (i = 0; i < ainfo->b; i++) {
        bibd[i] = (typeof(*bibd)) malloc(sizeof(typeof(**bibd)) * ainfo->k);

        for (j = 0; j < ainfo->k; j++) {
            fscanf(bibd_f, "%d", &bibd[i][j]);
        }
    }

    spd = (typeof(spd)) malloc(sizeof(typeof(*spd)) * ainfo->g * (ainfo->g - 1));

    for (i = 0; i < ainfo->g * (ainfo->g - 1); i++) {
        spd[i] = (typeof(*spd)) malloc(sizeof(typeof(**spd)) * ainfo->k);

        for (j = 0; j < ainfo->k; j++) {
            int a, b;
            a = i / ainfo->g;
            b = i % ainfo->g;
            spd[i][j] = (b + a * j) % ainfo->g;
        }
    }

    ainfo->bibd = bibd;
    ainfo->spd = spd;

    makeSubRAID(ainfo);
}

void destroy_addr_info(struct addr_info *ainfo) {
    int i;
    int stripe_nums = ainfo->v * ainfo->g * ainfo->r * (ainfo->g - 1) / ainfo->k;

    for (i = 0; i < stripe_nums; i++) {
        free(diskArray[i]);
        free(offsetArray[i]);
    }

    free(diskArray);
    free(offsetArray);

    for (i = 0; i < ainfo->v * ainfo->g; i++) {
        free(diskRegion[i]);
    }

    free(diskRegion);

    int **bibd = ainfo->bibd;

    for (i = 0; i < ainfo->b; i++) {
        free(bibd[i]);
    }

    free(bibd);

    int **spd = ainfo->spd;

    for (i = 0; i < ainfo->g * (ainfo->g - 1); i++) {
        free(spd[i]);
    }

    free(spd);

    free(ainfo);
}

void makeSubRAID(struct addr_info *ainfo) {
    int i, j, k;
    int **bibd = ainfo->bibd;
    int **spd = ainfo->spd;
    int *disk = (typeof(disk)) malloc(sizeof(typeof(disk)) * ainfo->g * ainfo->v);
    memset(disk, 0, sizeof(typeof(disk)) * ainfo->g * ainfo->v);

    int stripe_nums = ainfo->v * ainfo->g * ainfo->r * (ainfo->g - 1) / ainfo->k;
    int **bd;
    bd = (typeof(bd)) malloc(sizeof(typeof(*bd)) * stripe_nums);

    for (i = 0; i < stripe_nums; i++) {
        bd[i] = (typeof(*bd)) malloc(sizeof(typeof(**bd)) * ainfo->k);
    }

    for(i = 0; i < ainfo->b; i++) {
        for(j = 0; j < ainfo->g * (ainfo->g - 1); j++) {
            for(k = 0; k < ainfo->k; k++) {
                int a = bibd[i][k];
                int b = spd[j][k];
                bd[i * ainfo->g * (ainfo->g - 1) + j][k] = ainfo->g * a + b;
            }
        }
    }

    for(i = 0; i < stripe_nums; i++) {
        for(j = 0; j < ainfo->k; j++) {
            diskArray[i][j] = bd[i][j];
            offsetArray[i][j] = disk[bd[i][j]];
            diskRegion[bd[i][j]][disk[bd[i][j]]] = i;
            disk[bd[i][j]]++;

            if((disk[bd[i][j]] + 1) % ainfo->g == 0) {
                diskRegion[bd[i][j]][disk[bd[i][j]]] = -1;
                disk[bd[i][j]]++;
            }
        }
    }

    free(disk);

    for (i = 0; i < stripe_nums; i++) {
        free(bd[i]);
    }

    free(bd);
}


void oi_sub_raid_request(struct thr_info *tip, int subRAIDAddr, int disks[] , int offsets[], int reqSize, char op) {
    struct iocb *list[MAX_DEVICE_NUM];
    struct request_info reqs[MAX_DEVICE_NUM];

    struct addr_info *ainfo = tip->ainfo;

    int dataDiskNum = ainfo->k - 1;
    int stripeId;
    int inStripeAddr, inBlockId;       //data的位置，在条带内部
    int blockId[4], diskId[4];     //全磁盘， 可能涉及到4个块，1个data和3个parity
    int reqBlockNum;

    int virDiskId[2]; //虚拟磁盘号：0,1或2

    int groupId, regionId;     //修改的数据或global parity所在的组号
    int inRegionX, inRegionY;

    int localX;   //对应的local parity的相对磁盘号，相对region号都是2

    if(reqSize % BLOCK == 0) {
        reqBlockNum = reqSize / BLOCK;

    } else {
        reqBlockNum = reqSize / BLOCK + 1;
    }

    int i, req_count;

    for(i = 0; i < reqBlockNum; i++) {
        req_count = 0;

        stripeId = subRAIDAddr / ((dataDiskNum + 1) * dataDiskNum);

        inStripeAddr = subRAIDAddr % ((dataDiskNum + 1) * dataDiskNum);
        inBlockId = inStripeAddr / (dataDiskNum + 1);

        virDiskId[0] = inStripeAddr % (dataDiskNum + 1);
        diskId[0] = disks[virDiskId[0]];

        if(virDiskId[0] >= dataDiskNum - inBlockId) { //****这里就完成了轮转
            inBlockId += 1;
        }

        blockId[0] = offsets[virDiskId[0]] * ainfo->blocks_partition + stripeId * (dataDiskNum + 1) + inBlockId;


        int ntodo = 0, ndone;
        reqs[req_count].type = 1;
        reqs[req_count].disk_num = diskId[0];
        reqs[req_count].offset = blockId[0] * BLOCK;
        reqs[req_count].size = BLOCK;
        reqs[req_count].stripe_id = -1;
        req_count++;
        ntodo++;

        if(op == 'w' || op == 'W') {
            reqs[req_count].type = 0;
            reqs[req_count].disk_num = diskId[0];
            reqs[req_count].offset = blockId[0] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            req_count++;
            ntodo++;

            // 1.  global parity
            virDiskId[1] = dataDiskNum - inBlockId;
            diskId[1] = disks[virDiskId[1]];
            blockId[1] = offsets[virDiskId[1]] * ainfo->blocks_partition + stripeId * (dataDiskNum + 1) + inBlockId;

            reqs[req_count].type = 1;
            reqs[req_count].disk_num = diskId[1];
            reqs[req_count].offset = blockId[1] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            req_count++;
            ntodo++;

            reqs[req_count].type = 0;
            reqs[req_count].disk_num = diskId[1];
            reqs[req_count].offset = blockId[1] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            req_count++;
            ntodo++;

            // 2.  data对应的local parity
            groupId = disks[virDiskId[0]] / ainfo->g;
            regionId = offsets[virDiskId[0]] / ainfo->g;

            inRegionX = disks[virDiskId[0]] % ainfo->g;
            inRegionY = offsets[virDiskId[0]] % ainfo->g;
            localX = ((inRegionX - inRegionY) + ainfo->g - 1) % ainfo->g;

            diskId[2] = groupId * ainfo->g + localX;
            blockId[2] = (regionId * ainfo->g + ainfo->g - 1) * ainfo->blocks_partition + stripeId * (dataDiskNum + 1) + inBlockId;

            reqs[req_count].type = 1;
            reqs[req_count].disk_num = diskId[2];
            reqs[req_count].offset = blockId[2] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            req_count++;
            ntodo++;

            reqs[req_count].type = 0;
            reqs[req_count].disk_num = diskId[2];
            reqs[req_count].offset = blockId[2] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            req_count++;
            ntodo++;

            // 3.  global parity对应的local parity
            groupId = disks[virDiskId[1]] / ainfo->g;
            regionId = offsets[virDiskId[1]] / ainfo->g;

            inRegionX = disks[virDiskId[1]] % ainfo->g;
            inRegionY = offsets[virDiskId[1]] % ainfo->g;
            localX = ((inRegionX - inRegionY) + ainfo->g - 1) % ainfo->g;

            diskId[3] = groupId * ainfo->g + localX;
            blockId[3] = (regionId * ainfo->g + ainfo->g - 1) * ainfo->blocks_partition + stripeId * (dataDiskNum + 1) + inBlockId;

            reqs[req_count].type = 1;
            reqs[req_count].disk_num = diskId[3];
            reqs[req_count].offset = blockId[3] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            req_count++;
            ntodo++;

            reqs[req_count].type = 0;
            reqs[req_count].disk_num = diskId[3];
            reqs[req_count].offset = blockId[3] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            req_count++;
            ntodo++;
        }

        iocbs_map(tip, list, reqs, ntodo, 0);

        ndone = io_submit(tip->ctx, ntodo, list);

        if (ndone != ntodo) {
            fatal("io_submit", ERR_SYSCALL,
                  "%d: io_submit(%d:%ld) failed (%s)\n",
                  tip->cpu, ntodo, ndone,
                  strerror(labs(ndone)));
            /*NOTREACHED*/
        }

        pthread_mutex_lock(&tip->mutex);
        tip->naios_out += ndone;
        assert(tip->naios_out <= naios);

        if (tip->reap_wait) {
            tip->reap_wait = 0;
            pthread_cond_signal(&tip->cond);
        }
        pthread_mutex_unlock(&tip->mutex);

        subRAIDAddr++;
    }
}

//访问oi-raid
void oi_raid_request(struct thr_info *tip, int logicAddr, int reqSize, char op ) {
    int i;
    int subRAIDId;
    int subRAIDAddr;

    int reqBlockNum;

    int disks[MAX_DEVICE_NUM], offsets[MAX_DEVICE_NUM];
    struct addr_info *ainfo = tip->ainfo;

    subRAIDId = logicAddr / (ainfo->blocks_partition * (ainfo->k - 1));
    subRAIDAddr = logicAddr % (ainfo->blocks_partition * (ainfo->k - 1));

    if(reqSize % BLOCK == 0) {
        reqBlockNum = reqSize / BLOCK;
    } else {
        reqBlockNum = reqSize / BLOCK + 1;
    }

    for(i = 0; i < ainfo->k; i++) {
        disks[i] = diskArray[subRAIDId][i];
        offsets[i] = offsetArray[subRAIDId][i];
    }

    if(subRAIDAddr + reqBlockNum <= (ainfo->blocks_partition * (ainfo->k - 1))) {
        oi_sub_raid_request(tip, subRAIDAddr, disks, offsets, reqSize, op);

    } else {
        int reqSizeFirst, reqSizeLast;
        reqSizeFirst = ((ainfo->blocks_partition * (ainfo->k - 1)) - subRAIDAddr) * BLOCK;
        oi_sub_raid_request(tip, subRAIDAddr, disks, offsets, reqSizeFirst, op);

        for(i = 0; i < ainfo->k; i++) {
            disks[i] = diskArray[subRAIDId + 1][i];
            offsets[i] = offsetArray[subRAIDId + 1][i];
        }

        reqSizeLast = (subRAIDAddr + reqBlockNum - (ainfo->blocks_partition * (ainfo->k - 1))) * BLOCK;
        oi_sub_raid_request(tip, 0, disks, offsets, reqSizeLast, op);
    }
}


//访问21个磁盘的RAID5盘阵，每3个磁盘为一个2+1的RAID5
void raid5_3time7disks_request(struct thr_info *tip, int logicAddr, int reqSize, char op) {
    struct iocb *list[MAX_DEVICE_NUM];
    struct request_info reqs[MAX_DEVICE_NUM];

    struct addr_info *ainfo = tip->ainfo;

    int dataDiskNum = ainfo->k - 1;
    int dataPerStripe = (dataDiskNum + 1) * dataDiskNum;
    int maxOffset, reqBlockNum;
    int stripeId, groupId, inStripeAddr, inBlockId, diskId, blockId, sectorId;

    maxOffset = ainfo->capacity_total;

    if(reqSize % BLOCK == 0) {
        reqBlockNum = reqSize / BLOCK;

    } else {
        reqBlockNum = reqSize / BLOCK + 1;
    }

    int groups = ainfo->disk_nums / ainfo->k;

    int i, req_count;

    for(i = 0; i < reqBlockNum; i++) {
        if (logicAddr < maxOffset) {
            req_count = 0;

            stripeId = logicAddr / (dataPerStripe * groups);
            groupId = (logicAddr % (dataPerStripe * groups)) / dataPerStripe;
            inStripeAddr = logicAddr % dataPerStripe;
            inBlockId = inStripeAddr / (dataDiskNum + 1);

            diskId = inStripeAddr % (dataDiskNum + 1);

            if (diskId >= dataDiskNum - inBlockId) { //****这里就完成了轮转
                inBlockId += 1;
            }

            diskId += groupId * ainfo->k;
            blockId = stripeId * (dataDiskNum + 1) + inBlockId;

            int ntodo = 0, ndone;
            reqs[req_count].type = 1;
            reqs[req_count].disk_num = diskId;
            reqs[req_count].offset = blockId * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            req_count++;
            ntodo++;

            if (op == 'w' || op == 'W') {
                reqs[req_count].type = 0;
                reqs[req_count].disk_num = diskId;
                reqs[req_count].offset = blockId * BLOCK;
                reqs[req_count].size = BLOCK;
                reqs[req_count].stripe_id = -1;
                req_count++;
                ntodo++;

                reqs[req_count].type = 1;
                reqs[req_count].disk_num = dataDiskNum - inBlockId + groupId * ainfo->k;
                reqs[req_count].offset = blockId * BLOCK;
                reqs[req_count].size = BLOCK;
                reqs[req_count].stripe_id = -1;
                req_count++;
                ntodo++;

                reqs[req_count].type = 0;
                reqs[req_count].disk_num = dataDiskNum - inBlockId + groupId * ainfo->k;
                reqs[req_count].offset = blockId * BLOCK;
                reqs[req_count].size = BLOCK;
                reqs[req_count].stripe_id = -1;
                req_count++;
                ntodo++;
            }

            iocbs_map(tip, list, reqs, ntodo, 0);

            ndone = io_submit(tip->ctx, ntodo, list);

            if (ndone != ntodo) {
                fatal("io_submit", ERR_SYSCALL,
                      "%d: io_submit(%d:%ld) failed (%s)\n",
                      tip->cpu, ntodo, ndone,
                      strerror(labs(ndone)));
                /*NOTREACHED*/
            }

            pthread_mutex_lock(&tip->mutex);
            tip->naios_out += ndone;
            assert(tip->naios_out <= naios);

            if (tip->reap_wait) {
                tip->reap_wait = 0;
                pthread_cond_signal(&tip->cond);
            }

            pthread_mutex_unlock(&tip->mutex);


            logicAddr++;
        }
    }
}

inline int is_finish(struct addr_info *ainfo, long long start_time) {
    start_time = gettime() - start_time;

    if (start_time > (long long) ainfo->running_time * 1000 * 1000 * 1000)
        return 1;

    else
        return 0;
}

//g=k
// 21个磁盘，部署7组传统2+1 RAID5，假定每个磁盘6个PARTITION
void raid5_online(struct thr_info *tip) {
    struct addr_info *ainfo = tip->ainfo;

    int hostName, logicAddr, size;
    char op;
    double timeStamp;

    FILE *f = fopen(ainfo->trace_fn, "r");
    long long last_time = gettime();
    int req_count = 0;

    while (!is_finish(ainfo, last_time)) {
        if ((req_count + 1) % 20 == 0)
            fprintf(stderr, "has process %d request\n", req_count);

        int retCode;
        retCode = fscanf(f, "%d,%d,%d,%c,%lf", &hostName, &logicAddr, &size, &op, &timeStamp);

        //while (retCode == 5){
        //  retCode = fscanf(f, "%d,%d,%d,%c,%lf", &hostName, &logicAddr, &size, &op, &timeStamp);;
        //}
        if (retCode != 5)
            break;

        long long cur_time = gettime();
        long long time_diff = (long long) (timeStamp * 1000 * 1000 * 1000) - (cur_time - last_time);

        if (time_diff > 1000) {
            usleep(time_diff / 1000);
        }

        logicAddr = (logicAddr / 8) % ainfo->capacity_total;
        raid5_3time7disks_request(tip, logicAddr, size, op);
        req_count++;
    }

    fclose(f);
}

//oi-raid单盘修复
void oi_raid_online(struct thr_info *tip) {
    struct addr_info *ainfo = tip->ainfo;

    int hostName, logicAddr, size;
    char op;
    double timeStamp;

    FILE *f = fopen(ainfo->trace_fn, "r");
    long long last_time = gettime();
    int req_count = 0;

    while (!is_finish(ainfo, last_time)) {
        if ((req_count + 1) % 20 == 0)
            fprintf(stderr, "has process %d request\n", req_count);

        int retCode;
        retCode = fscanf(f, "%d,%d,%d,%c,%lf", &hostName, &logicAddr, &size, &op, &timeStamp);

        //while (retCode == 5){
        //  retCode = fscanf(f, "%d,%d,%d,%c,%lf", &hostName, &logicAddr, &size, &op, &timeStamp);;
        //}
        if (retCode != 5)
            break;

        long long cur_time = gettime();
        long long time_diff = (long long) (timeStamp * 1000 * 1000 * 1000) - (cur_time - last_time);

        if (time_diff > 1000) {
            usleep(time_diff / 1000);
        }

        logicAddr = (logicAddr / 8) % ainfo->capacity_total;
        oi_raid_request(tip, logicAddr, size, op);
        req_count++;
    }

    fclose(f);
}
