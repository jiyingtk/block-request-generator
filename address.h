//#define DEGRADED
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

    int n, m; //RS Code, n data, m parity
    int g2; //S2-RAID

    int running_time;
    char *trace_fn;
};

int requestPerSecond = 0;

int **diskArray;           //子阵列对应的 磁盘号
int **offsetArray;         //子阵列对应的 偏移量（分区号）
int **diskRegion;           //每个disk包含的Region号

void makeSubRAID(struct addr_info *ainfo);

void init_parameters(struct addr_info *ainfo) {
    ainfo->blocks_per_strip = ainfo->strip_size / BLOCK;

    ainfo->capacity /= ainfo->strip_size;
    ainfo->capacity *= ainfo->strip_size;
   
    ainfo->stripe_nums = 1;
    
    if (ainfo->method == 0) {   //RAID5
        ainfo->disk_nums = ainfo->disk_nums / ainfo->k * ainfo->k;
        ainfo->g2 = ainfo->disk_nums / ainfo->k;
        ainfo->stripe_nums = ainfo->g2 * ainfo->g2;

        ainfo->capacity /= ainfo->strip_size;

        int align = ainfo->g2;
        ainfo->capacity /= align;
        ainfo->capacity *= align;

        ainfo->strips_partition = ainfo->capacity / ainfo->g2;
        ainfo->blocks_partition = ainfo->strips_partition * ainfo->blocks_per_strip;

        ainfo->data_blocks = ainfo->capacity;
        ainfo->data_blocks *= ainfo->blocks_per_strip;

        ainfo->capacity *= ainfo->strip_size;

        ainfo->capacity_total = ainfo->capacity / BLOCK * ainfo->disk_nums / ainfo->k * (ainfo->k - 1);
    } else if (ainfo->method == 1) {    //OI-RAID
        ainfo->stripe_nums = ainfo->v * ainfo->g * ainfo->r * (ainfo->g - 1) / ainfo->k;

        ainfo->capacity /= ainfo->strip_size;

        ainfo->capacity /= ainfo->g * ainfo->r;
        ainfo->capacity *= ainfo->g * ainfo->r;

        ainfo->strips_partition = (ainfo->capacity) / (ainfo->g * ainfo->r);
        ainfo->blocks_partition = ainfo->strips_partition * ainfo->blocks_per_strip;

        ainfo->data_blocks = ainfo->capacity;
        ainfo->data_blocks *= ainfo->blocks_per_strip;

        ainfo->capacity *= ainfo->strip_size;

        ainfo->capacity_total = ainfo->stripe_nums * (ainfo->k - 1) * ainfo->blocks_partition;
    } else if (ainfo->method == 2) {    //RS Code
        ainfo->disk_nums = ainfo->disk_nums / (ainfo->n + ainfo->m) * (ainfo->n + ainfo->m);
        ainfo->g2 = ainfo->disk_nums / (ainfo->n + ainfo->m);
        ainfo->stripe_nums = ainfo->g2 * ainfo->g2;

        ainfo->capacity /= ainfo->strip_size;

        int align = ainfo->g2;
        ainfo->capacity /= align;
        ainfo->capacity *= align;

        ainfo->strips_partition = ainfo->capacity / ainfo->g2;
        ainfo->blocks_partition = ainfo->strips_partition * ainfo->blocks_per_strip;

        ainfo->data_blocks = ainfo->capacity;
        ainfo->data_blocks *= ainfo->blocks_per_strip;

        ainfo->capacity *= ainfo->strip_size;

        ainfo->capacity_total = ainfo->capacity / BLOCK * ainfo->disk_nums / (ainfo->n + ainfo->m) * (ainfo->n);
    } else if (ainfo->method == 3) {    //S2-RAID
        ainfo->disk_nums = ainfo->disk_nums / ainfo->k * ainfo->k;
        ainfo->g2 = ainfo->disk_nums / ainfo->k;
        ainfo->stripe_nums = ainfo->g2 * ainfo->g2;

        ainfo->capacity /= ainfo->strip_size;

        int align = ainfo->g2;
        ainfo->capacity /= align;
        ainfo->capacity *= align;

        ainfo->strips_partition = ainfo->capacity / ainfo->g2;
        ainfo->blocks_partition = ainfo->strips_partition * ainfo->blocks_per_strip;

        ainfo->data_blocks = ainfo->capacity;
        ainfo->data_blocks *= ainfo->blocks_per_strip;

        ainfo->capacity *= ainfo->strip_size;

        ainfo->capacity_total = ainfo->capacity / BLOCK * ainfo->disk_nums / ainfo->k * (ainfo->k - 1);
        
    } else if (ainfo->method == 4) {    //Parity Declustering
        ainfo->stripe_nums = ainfo->b;

        ainfo->capacity /= ainfo->strip_size;

        int align = ainfo->r;
        ainfo->capacity /= align;
        ainfo->capacity *= align;

        ainfo->strips_partition = ainfo->capacity / ainfo->r;
        ainfo->blocks_partition = ainfo->strips_partition * ainfo->blocks_per_strip;

        ainfo->data_blocks = ainfo->capacity;
        ainfo->data_blocks *= ainfo->blocks_per_strip;

        ainfo->capacity *= ainfo->strip_size;

        ainfo->capacity_total = ainfo->capacity / BLOCK * ainfo->disk_nums / ainfo->k * (ainfo->k - 1);

    } else {
        exit(1);
    }

    fprintf(stderr, "capacity %fGB\n", ainfo->capacity * 1.0f / 1024 / 1024 / 1024);
   
}

void init_addr_info(struct addr_info *ainfo) {
    char fn[128];
    sprintf(fn, "%d.%d.bd", ainfo->v, ainfo->k);
    FILE *bibd_f = fopen(fn, "r");

    fscanf(bibd_f, "%d %d %d %d %d", &ainfo->b, &ainfo->v, &ainfo->k, &ainfo->r, &ainfo->lambda);

    ainfo->disk_nums = ainfo->v * ainfo->g;

    if (ainfo->method == 4) {
        fclose(bibd_f);

        sprintf(fn, "%d.%d.bd", ainfo->disk_nums, ainfo->k);
        bibd_f = fopen(fn, "r");
        fscanf(bibd_f, "%d %d %d %d %d", &ainfo->b, &ainfo->v, &ainfo->k, &ainfo->r, &ainfo->lambda);

        //fprintf(stderr, "%d %d %d %d %d\n", ainfo->b, ainfo->v, ainfo->k, ainfo->r, ainfo->lambda);
        //exit(1);
    }

    init_parameters(ainfo);

    int i, j;
    int stripe_nums = ainfo->stripe_nums, region_nums = 1;
    int totalDiskNum = ainfo->method != 2 ? ainfo->k : ainfo->n + ainfo->m;
    
    if (ainfo->method == 1)
        region_nums = ainfo->g * ainfo->r;
    else if (ainfo->method == 0 || ainfo->method == 2 || ainfo->method == 3)
        region_nums = ainfo->g2;
    else if (ainfo->method == 4)
        region_nums = ainfo->r;

        diskArray = (typeof(diskArray)) malloc(sizeof(typeof(*diskArray)) * stripe_nums);
        offsetArray = (typeof(offsetArray)) malloc(sizeof(typeof(*offsetArray)) * stripe_nums);

        for (i = 0; i < stripe_nums; i++) {
            diskArray[i] = (typeof(*diskArray)) malloc(sizeof(typeof(**diskArray)) * totalDiskNum);
            offsetArray[i] = (typeof(*offsetArray)) malloc(sizeof(typeof(**offsetArray)) * totalDiskNum);
        }

        diskRegion = (typeof(diskRegion)) malloc(sizeof(typeof(*diskRegion)) * ainfo->disk_nums);

        for (i = 0; i < ainfo->disk_nums; i++) {
            diskRegion[i] = (typeof(*diskRegion)) malloc(sizeof(typeof(**diskRegion)) * region_nums);
        }

    int **bibd, **spd;
    bibd = (typeof(bibd)) malloc(sizeof(typeof(*bibd)) * ainfo->b);

    for (i = 0; i < ainfo->b; i++) {
        bibd[i] = (typeof(*bibd)) malloc(sizeof(typeof(**bibd)) * ainfo->k);

        for (j = 0; j < ainfo->k; j++) {
            fscanf(bibd_f, "%d", &bibd[i][j]);
        }
    }

    int g = ainfo->method == 3 ? ainfo->g2 : ainfo->g;

    spd = (typeof(spd)) malloc(sizeof(typeof(*spd)) * g * g);

    for (i = 0; i < g * g; i++) {
        spd[i] = (typeof(*spd)) malloc(sizeof(typeof(**spd)) * ainfo->k);

        for (j = 0; j < ainfo->k; j++) {
            int a, b;
            a = i / g;
            b = i % g;
            spd[i][j] = (b + a * j) % g;
        }
    }

    ainfo->bibd = bibd;
    ainfo->spd = spd;

    if (ainfo->method == 0 || ainfo->method == 2) {
        for(i = 0; i < stripe_nums; i++) {
            for(j = 0; j < totalDiskNum; j++) {
                diskArray[i][j] = (i % ainfo->g2) * totalDiskNum + j;
                offsetArray[i][j] = i / ainfo->g2;
                diskRegion[diskArray[i][j]][offsetArray[i][j]] = i;
            }
        }
    }
    else if (ainfo->method == 1)
        makeSubRAID(ainfo);
    else if (ainfo->method == 3) {
        for(i = 0; i < stripe_nums; i++) {
            for(j = 0; j < ainfo->k; j++) {
                diskArray[i][j] = spd[i][j] + j * ainfo->g2;
                offsetArray[i][j] = i / ainfo->g2;
                diskRegion[diskArray[i][j]][offsetArray[i][j]] = i;
            }
        }
    }
    else if (ainfo->method == 4) {
        int disk[MAX_DEVICE_NUM] = {0};
        for(i = 0; i < stripe_nums; i++) {
            for(j = 0; j < ainfo->k; j++) {
                diskArray[i][j] = bibd[i][j];
                offsetArray[i][j] = disk[diskArray[i][j]];
                disk[diskArray[i][j]]++;

                diskRegion[diskArray[i][j]][offsetArray[i][j]] = i;
            }
        }
    }
}

void destroy_addr_info(struct addr_info *ainfo) {
    int i;
    int stripe_nums = ainfo->stripe_nums;

    for (i = 0; i < stripe_nums; i++) {
        free(diskArray[i]);
        free(offsetArray[i]);
    }

    free(diskArray);
    free(offsetArray);

    for (i = 0; i < ainfo->disk_nums; i++) {
        free(diskRegion[i]);
    }

    free(diskRegion);

    int **bibd = ainfo->bibd;

    for (i = 0; i < ainfo->b; i++) {
        free(bibd[i]);
    }

    free(bibd);

    int **spd = ainfo->spd;
    int g = ainfo->method == 3 ? ainfo->g2 : ainfo->g;

    for (i = 0; i < g * g; i++) {
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


void sub_raid_request(struct thr_info *tip, int subRAIDAddr, int disks[] , int offsets[], int reqSize, char op) {
    struct iocb *list[MAX_DEVICE_NUM];
    struct request_info reqs[MAX_DEVICE_NUM];

    struct addr_info *ainfo = tip->ainfo;

    int dataDiskNum = ainfo->k - 1;
    int stripeId;
    int inStripeAddr, inBlockId;       //data的位置，在条带内部
    int diskId[4];     //全磁盘， 可能涉及到4个块，1个data和3个parity
    addr_type blockId[4]; 
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

        long long start_time = gettime();
        int ntodo = 0, ndone;

#ifndef DEGRADED
        reqs[req_count].type = 1;
        reqs[req_count].disk_num = diskId[0];
        reqs[req_count].offset = blockId[0] * BLOCK;
        reqs[req_count].size = BLOCK;
        reqs[req_count].stripe_id = -1;
        reqs[req_count].start_time = start_time;
        reqs[req_count].original_op = 'r';
        req_count++;
        ntodo++;
#else
        int read_reqs = 0;
        if (diskId[0] == ainfo->failedDisk && (op == 'r' || op == 'R')) {
            int j;
            for (j = 0; j < ainfo->k; j++) {
                if (disks[j] != ainfo->failedDisk) {
                    blockId[0] = offsets[j] * ainfo->blocks_partition + stripeId * (dataDiskNum + 1) + inBlockId;
                    reqs[req_count].type = 1;
                    reqs[req_count].disk_num = disks[j];
                    reqs[req_count].offset = blockId[0] * BLOCK;
                    reqs[req_count].size = BLOCK;
                    reqs[req_count].stripe_id = -1;
                    reqs[req_count].start_time = start_time;
                    reqs[req_count].original_op = 'r';
                    req_count++;
                    ntodo++;
                    read_reqs++;
                }
            }
            fprintf(stderr, "degraded read %d\n", read_reqs);    
        }
        else {
            reqs[req_count].type = 1;
            reqs[req_count].disk_num = diskId[0];
            reqs[req_count].offset = blockId[0] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            reqs[req_count].start_time = start_time;
            reqs[req_count].original_op = 'r';
            req_count++;
            ntodo++;
            read_reqs++;
        }
#endif


        pthread_mutex_lock(&tip->mutex);
        if (op == 'r' || op == 'R') 
#ifndef DEGRADED
            hash_add(tip->ht, start_time, 1);
#else
            hash_add(tip->ht, start_time, read_reqs);
#endif
        else if (ainfo->method == 1) //OI-RAID
            hash_add(tip->ht, start_time, 8);
        else    //S2-RAID, Parity Declustering
            hash_add(tip->ht, start_time, 4);

        pthread_mutex_unlock(&tip->mutex);

        if(op == 'w' || op == 'W') {
            reqs[req_count - 1].original_op = 'w';

            reqs[req_count].type = 0;
            reqs[req_count].disk_num = diskId[0];
            reqs[req_count].offset = blockId[0] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            reqs[req_count].start_time = start_time;
            reqs[req_count].original_op = 'w';
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
            reqs[req_count].start_time = start_time;
            reqs[req_count].original_op = 'w';
            req_count++;
            ntodo++;

            reqs[req_count].type = 0;
            reqs[req_count].disk_num = diskId[1];
            reqs[req_count].offset = blockId[1] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            reqs[req_count].start_time = start_time;
            reqs[req_count].original_op = 'w';
            req_count++;
            ntodo++;

        if (ainfo->method == 1) {

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
            reqs[req_count].start_time = start_time;
            reqs[req_count].original_op = 'w';
            req_count++;
            ntodo++;

            reqs[req_count].type = 0;
            reqs[req_count].disk_num = diskId[2];
            reqs[req_count].offset = blockId[2] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            reqs[req_count].start_time = start_time;
            reqs[req_count].original_op = 'w';
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
            reqs[req_count].start_time = start_time;
            reqs[req_count].original_op = 'w';
            req_count++;
            ntodo++;

            reqs[req_count].type = 0;
            reqs[req_count].disk_num = diskId[3];
            reqs[req_count].offset = blockId[3] * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            reqs[req_count].start_time = start_time;
            reqs[req_count].original_op = 'w';
            req_count++;
            ntodo++;

        }

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


void rs_request(struct thr_info *tip, int logicAddr, int disks[] , int offsets[], int reqSize, char op) {
    struct iocb *list[MAX_DEVICE_NUM];
    struct request_info reqs[MAX_DEVICE_NUM];

    struct addr_info *ainfo = tip->ainfo;

    int dataDiskNum = ainfo->n;
    int dataPerStripe = (dataDiskNum + ainfo->m) * dataDiskNum;
    int maxOffset, reqBlockNum;
    int stripeId, groupId, inStripeAddr, inBlockId, diskId, ectorId;
    addr_type blockId;

    maxOffset = ainfo->blocks_partition * ainfo->n;

    if(reqSize % BLOCK == 0) {
        reqBlockNum = reqSize / BLOCK;
    } else {
        reqBlockNum = reqSize / BLOCK + 1;
    }

    int groups = 1;

    int i, req_count;

    for(i = 0; i < reqBlockNum; i++) {
        if (logicAddr < maxOffset) {
            req_count = 0;

            stripeId = logicAddr / (dataPerStripe * groups);
            groupId = (logicAddr % (dataPerStripe * groups)) / dataPerStripe;
            inStripeAddr = logicAddr % dataPerStripe;
            inBlockId = inStripeAddr / (dataDiskNum + ainfo->m);

            diskId = inStripeAddr % (dataDiskNum + ainfo->m);

            if (diskId >= dataDiskNum - inBlockId) { //****这里就完成了轮转
                if (dataDiskNum < diskId)
                    inBlockId += dataDiskNum + ainfo->m - diskId;
                else
                    inBlockId += ainfo->m;
            }
            
            int diskIdInGroup = diskId;

            diskId += groupId * (ainfo->n + ainfo->m);
            blockId = stripeId * (ainfo->n + ainfo->m) + inBlockId;

            long long start_time = gettime();
            int ntodo = 0, ndone;


#ifndef DEGRADED
            reqs[req_count].type = 1;
            reqs[req_count].disk_num = disks[diskId];
            reqs[req_count].offset = (blockId + offsets[diskId] * ainfo->blocks_partition) * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            reqs[req_count].start_time = start_time;
            reqs[req_count].original_op = 'r';
            req_count++;
            ntodo++;
#else
            int read_reqs = 0;
            if (diskId == ainfo->failedDisk && (op == 'r' || op == 'R')) {
                int k;
                for (k = 1; k <= ainfo->n; k++) {
                    reqs[req_count].type = 1;
                    int diskId2 = (diskIdInGroup + k) % (ainfo->n + ainfo->m) + groupId * (ainfo->n + ainfo->m);
                    reqs[req_count].disk_num = disks[diskId2];
                    reqs[req_count].offset = (blockId + offsets[diskId2] * ainfo->blocks_partition) * BLOCK;
                    reqs[req_count].size = BLOCK;
                    reqs[req_count].stripe_id = -1;
                    reqs[req_count].start_time = start_time;
                    reqs[req_count].original_op = 'r';
                    req_count++;
                    ntodo++;
                    read_reqs++;
                }
            fprintf(stderr, "degraded read %d\n", read_reqs);    
            }
            else {
                reqs[req_count].type = 1;
                reqs[req_count].disk_num = disks[diskId];
                reqs[req_count].offset = (blockId + offsets[diskId] * ainfo->blocks_partition) * BLOCK;
                reqs[req_count].size = BLOCK;
                reqs[req_count].stripe_id = -1;
                reqs[req_count].start_time = start_time;
                reqs[req_count].original_op = 'r';
                req_count++;
                ntodo++;
                read_reqs++;
            }
#endif

            pthread_mutex_lock(&tip->mutex);
            if (op == 'r' || op == 'R') 
#ifndef DEGRADED
                hash_add(tip->ht, start_time, 1);
#else
                hash_add(tip->ht, start_time, read_reqs);
#endif
            else
                hash_add(tip->ht, start_time, 2 * ainfo->m + 2);
            pthread_mutex_unlock(&tip->mutex);

            if (op == 'w' || op == 'W') {
                reqs[req_count - 1].original_op = 'w';

                reqs[req_count].type = 0;
                reqs[req_count].disk_num = disks[diskId];
                reqs[req_count].offset = (blockId + offsets[diskId] * ainfo->blocks_partition) * BLOCK;
                reqs[req_count].size = BLOCK;
                reqs[req_count].stripe_id = -1;
                reqs[req_count].start_time = start_time;
                reqs[req_count].original_op = 'w';
                req_count++;
                ntodo++;


                int k;
                int pdisk = (ainfo->n + ainfo->m + dataDiskNum - inBlockId) % (ainfo->n + ainfo->m);
                for (k = 0; k < ainfo->m; k++) {
                    int diskId2 = (pdisk + k) % (ainfo->n + ainfo->m) + groupId * (ainfo->n + ainfo->m);
                    reqs[req_count].type = 1;
                    reqs[req_count].disk_num = disks[diskId2];
                    reqs[req_count].offset = (blockId + offsets[diskId2] * ainfo->blocks_partition) * BLOCK;
                    reqs[req_count].size = BLOCK;
                    reqs[req_count].stripe_id = -1;
                    reqs[req_count].start_time = start_time;
                    reqs[req_count].original_op = 'w';
                    req_count++;
                    ntodo++;

                    reqs[req_count].type = 0;
                    reqs[req_count].disk_num = disks[diskId2];
                    reqs[req_count].offset = (blockId + offsets[diskId2] * ainfo->blocks_partition) * BLOCK;
                    reqs[req_count].size = BLOCK;
                    reqs[req_count].stripe_id = -1;
                    reqs[req_count].start_time = start_time;
                    reqs[req_count].original_op = 'w';
                    req_count++;
                    ntodo++;
                }

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

//访问21个磁盘的RAID5盘阵，每3个磁盘为一个2+1的RAID5
void raid5_3time7disks_request(struct thr_info *tip, int logicAddr, int reqSize, char op) {
    struct iocb *list[MAX_DEVICE_NUM];
    struct request_info reqs[MAX_DEVICE_NUM];

    struct addr_info *ainfo = tip->ainfo;

    int dataDiskNum = ainfo->k - 1;
    int dataPerStripe = (dataDiskNum + 1) * dataDiskNum;
    int maxOffset, reqBlockNum;
    int stripeId, groupId, inStripeAddr, inBlockId, diskId, ectorId;
    addr_type blockId;

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
            int diskIdInGroup = diskId;

            diskId += groupId * ainfo->k;
            blockId = stripeId * (dataDiskNum + 1) + inBlockId;

            long long start_time = gettime();

            int ntodo = 0, ndone;
#ifndef DEGRADED
            reqs[req_count].type = 1;
            reqs[req_count].disk_num = diskId;
            reqs[req_count].offset = blockId * BLOCK;
            reqs[req_count].size = BLOCK;
            reqs[req_count].stripe_id = -1;
            reqs[req_count].start_time = start_time;
            reqs[req_count].original_op = 'r';
            req_count++;
            ntodo++;
#else
            int read_reqs = 0;
            if (diskId == ainfo->failedDisk && (op == 'r' || op == 'R')) {
                int k;
                for (k = 1; k < ainfo->k; k++) {
                    reqs[req_count].type = 1;
                    reqs[req_count].disk_num = (diskIdInGroup + k) % ainfo->k + groupId * ainfo->k;
                    reqs[req_count].offset = blockId * BLOCK;
                    reqs[req_count].size = BLOCK;
                    reqs[req_count].stripe_id = -1;
                    reqs[req_count].start_time = start_time;
                    reqs[req_count].original_op = 'r';
                    req_count++;
                    ntodo++;
                    read_reqs++;
                }
                fprintf(stderr, "degraded read %d\n", read_reqs);    
            }
            else {
                reqs[req_count].type = 1;
                reqs[req_count].disk_num = diskId;
                reqs[req_count].offset = blockId * BLOCK;
                reqs[req_count].size = BLOCK;
                reqs[req_count].stripe_id = -1;
                reqs[req_count].start_time = start_time;
                reqs[req_count].original_op = 'r';
                req_count++;
                ntodo++;
                read_reqs++;
            }
#endif

            pthread_mutex_lock(&tip->mutex);
            if (op == 'r' || op == 'R') 
#ifndef DEGRADED
                hash_add(tip->ht, start_time, 1);
#else
                hash_add(tip->ht, start_time, read_reqs);
#endif
            else
                hash_add(tip->ht, start_time, 4); 
            pthread_mutex_unlock(&tip->mutex);


            if (op == 'w' || op == 'W') {
                reqs[req_count - 1].original_op = 'w';

                reqs[req_count].type = 0;
                reqs[req_count].disk_num = diskId;
                reqs[req_count].offset = blockId * BLOCK;
                reqs[req_count].size = BLOCK;
                reqs[req_count].stripe_id = -1;
                reqs[req_count].start_time = start_time;
                reqs[req_count].original_op = 'w';
                req_count++;
                ntodo++;

                reqs[req_count].type = 1;
                reqs[req_count].disk_num = dataDiskNum - inBlockId + groupId * ainfo->k;
                reqs[req_count].offset = blockId * BLOCK;
                reqs[req_count].size = BLOCK;
                reqs[req_count].stripe_id = -1;
                reqs[req_count].start_time = start_time;
                reqs[req_count].original_op = 'w';
                req_count++;
                ntodo++;

                reqs[req_count].type = 0;
                reqs[req_count].disk_num = dataDiskNum - inBlockId + groupId * ainfo->k;
                reqs[req_count].offset = blockId * BLOCK;
                reqs[req_count].size = BLOCK;
                reqs[req_count].stripe_id = -1;
                reqs[req_count].start_time = start_time;
                reqs[req_count].original_op = 'w';
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

void region_request(struct thr_info *tip, int logicAddr, int reqSize, char op ) {
    int i;
    int subRAIDId;
    int subRAIDAddr;

    int reqBlockNum;

    int disks[MAX_DEVICE_NUM], offsets[MAX_DEVICE_NUM];
    struct addr_info *ainfo = tip->ainfo;

    int dataDiskNum = ainfo->method != 2 ? ainfo->k - 1 : ainfo->n;
    int totalDiskNum = ainfo->method != 2 ? ainfo->k : ainfo->n + ainfo->m;

    subRAIDId = logicAddr / (ainfo->blocks_partition * dataDiskNum);
    subRAIDAddr = logicAddr % (ainfo->blocks_partition * dataDiskNum);

    if(reqSize % BLOCK == 0) {
        reqBlockNum = reqSize / BLOCK;
    } else {
        reqBlockNum = reqSize / BLOCK + 1;
    }

    for(i = 0; i < totalDiskNum; i++) {
        disks[i] = diskArray[subRAIDId][i];
        offsets[i] = offsetArray[subRAIDId][i];
    }

    if(subRAIDAddr + reqBlockNum <= (ainfo->blocks_partition * dataDiskNum)) {
        if (ainfo->method != 2)
            sub_raid_request(tip, subRAIDAddr, disks, offsets, reqSize, op);   
        else
            rs_request(tip, subRAIDAddr, disks, offsets, reqSize, op);   
    } else {
        int reqSizeFirst, reqSizeLast;
        reqSizeFirst = ((ainfo->blocks_partition * dataDiskNum) - subRAIDAddr) * BLOCK;
        if (ainfo->method != 2)
            sub_raid_request(tip, subRAIDAddr, disks, offsets, reqSizeFirst, op);
        else
            rs_request(tip, subRAIDAddr, disks, offsets, reqSizeFirst, op);
        
        reqSizeLast = (subRAIDAddr + reqBlockNum - (ainfo->blocks_partition * dataDiskNum)) * BLOCK;
        if (ainfo->method != 2)
            sub_raid_request(tip, 0, disks, offsets, reqSizeLast, op); 
        else
            rs_request(tip, 0, disks, offsets, reqSizeLast, op); 
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
void online(struct thr_info *tip) {
    struct addr_info *ainfo = tip->ainfo;

    int hostName, logicAddr, size;
    char op;
    double timeStamp;

    FILE *f = fopen(ainfo->trace_fn, "r");
    long long last_time = gettime();
    int req_count = 0;

    while (!is_finish(ainfo, last_time)) {
        if ((req_count + 1) % 2001 == 0)
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
        region_request(tip, logicAddr, size, op);
/*        switch (ainfo->method) {
            case 0:
                raid5_3time7disks_request(tip, logicAddr, size, op);
                break;
            case 1:
                region_request(tip, logicAddr, size, op);
                break;
            case 2:
                rs_request(tip, logicAddr, size, op);
                break;
            case 3:
                break;
            case 4:
                region_request(tip, logicAddr, size, op);
                break;
        }
*/
        req_count++;
    }

    fclose(f);
}

