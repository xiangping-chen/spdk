/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "bdev_raid.h"

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"

#include "spdk/log.h"

#include "libsdc.h"



static uint8_t libsdc_get_tgt(struct raid_bdev *raid_bdev, uint64_t LB_offset)
{
	uint8_t tgt = 0;
    uint64_t tgtId = 0;
    Ini_VolHandle *p_handle = (Ini_VolHandle *)raid_bdev->module_private;

    uint64_t tsc = 0, tsc1=0;
    static uint64_t latency = 0, cnt = 0;

    tsc = spdk_get_ticks();

    tgtId = LibSdc_Ioctl_GetLBTgt(*p_handle, LB_offset);

    tsc1= spdk_get_ticks();
    latency += (tsc1 - tsc);
    cnt ++;
    if (cnt %100000 == 0) {
    	SPDK_NOTICELOG ("avg latency %lu HZ\n", latency/cnt);
    	latency = 0;
    	cnt = 0;
    }

	tgt = tgtId & 0xFF;
    if (tgt >= raid_bdev->num_base_bdevs) {
    	SPDK_ERRLOG ("vol %s, LBA offset %lu, err get tgt %d\n", raid_bdev->bdev.name, LB_offset, tgt);
    	tgt = 0;
    } else {
    	SPDK_DEBUGLOG ("vol %s, LBA offset %lu Tgt %d\n", raid_bdev->bdev.name, LB_offset, tgt);
    }
    return tgt;
}

static int libsdc_init(struct raid_bdev *raid_bdev)
{
    LibSdc_Guid guid;
    LibSdc_NetAddress mdmAddress;
    LibSdc_Config sdcConfig;
    static int init = 0;

    if (init == 0) {
    	SPDK_NOTICELOG("Enter libsdc, init %d\n", init);
    	memset(&sdcConfig, 0, sizeof (sdcConfig));
    	memset(&mdmAddress, 0, sizeof (mdmAddress));

    	LibSdc_ParseGuid("00000000-0000-0000-0000-000000000001", &guid);
    	mdmAddress.ipv4 = LIBSDC_GENERATE_IPV4_ADDRESS(10, 228, 165, 120);
    	mdmAddress.port = 6611;
    	LIBSDC_SO_CONFIG_SET_PARAM(&sdcConfig, guid, guid);
    	LIBSDC_SO_CONFIG_SET_PARAM(&sdcConfig, mdm, mdmAddress);

    	if (!LibSdc_InitSimple(NULL, &sdcConfig, NULL, NULL, NULL)) {
    		SPDK_NOTICELOG("Failed libsdc init\n");
    		return -1;
    	}
    	if (!LibSdc_WaitForVolumes(30000)) {
    		SPDK_NOTICELOG("Failed getting volume list in 30 seconds\n");
    		return -1;
    	}
    	init = 1;
    }
    SPDK_NOTICELOG("Exit libsdc, init %d\n", init);
    return 0;
}

/*
 * brief:
 * raidx_bdev_io_completion function is called by lower layers to notify raid
 * module that particular bdev_io is completed.
 * params:
 * bdev_io - pointer to bdev io submitted to lower layers, like child io
 * success - bdev_io status
 * cb_arg - function callback context (parent raid_bdev_io)
 * returns:
 * none
 */
static void
raidx_bdev_io_completion(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid_bdev_io *raid_io = cb_arg;

	SPDK_DEBUGLOG("offset %lu, num_blocks %lu, type %d, ret %d\n",
			bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, bdev_io->type, success);

	spdk_bdev_free_io(bdev_io);

	if (success) {
		raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_SUCCESS);
	} else {
		raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static void
raidx_submit_rw_request(struct raid_bdev_io *raid_io);

static void
_raidx_submit_rw_request(void *_raid_io)
{
	struct raid_bdev_io *raid_io = _raid_io;

	raidx_submit_rw_request(raid_io);
}

/*
 * brief:
 * raidx_submit_rw_request function is used to submit I/O to the correct
 * member disk for raid0 bdevs.
 * params:
 * raid_io
 * returns:
 * none
 */
static void
raidx_submit_rw_request(struct raid_bdev_io *raid_io)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_bdev		*raid_bdev = raid_io->raid_bdev;
	uint8_t				pd_idx = 0;
	int				ret = 0;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	//* To add target lookup.
	pd_idx = libsdc_get_tgt(raid_bdev, bdev_io->u.bdev.offset_blocks);

	base_info = &raid_bdev->base_bdev_info[pd_idx];
	if (base_info->desc == NULL) {
		SPDK_ERRLOG("base bdev desc null for pd_idx %u\n", pd_idx);
		assert(0);
	}
	/*
	 * Submit child io to bdev layer with using base bdev descriptors, base
	 * bdev lba, base bdev child io length in blocks, buffer, completion
	 * function and function callback context
	 */
	assert(raid_ch != NULL);
	assert(raid_ch->base_channel);
	base_ch = raid_ch->base_channel[pd_idx];

	assert(base_info != NULL);
	assert(base_ch != NULL);
	if (bdev_io->type == SPDK_BDEV_IO_TYPE_READ) {
		ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
					     bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						 bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, raidx_bdev_io_completion,
					     raid_io);
	} else if (bdev_io->type == SPDK_BDEV_IO_TYPE_WRITE) {
		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
					      bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						  bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, raidx_bdev_io_completion,
					      raid_io);
	} else {
		SPDK_ERRLOG("Recvd not supported io type %u\n", bdev_io->type);
		assert(0);
	}
	SPDK_DEBUGLOG("offset %lu, num_blocks %lu, type %d, pd_index %d, ret %d\n",
			bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, bdev_io->type, pd_idx, ret);

	if (ret == -ENOMEM) {
		raid_bdev_queue_io_wait(raid_io, base_info->bdev, base_ch,
					_raidx_submit_rw_request);
	} else if (ret != 0) {
		SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
		assert(false);
		raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static void
raidx_submit_null_payload_request(struct raid_bdev_io *raid_io);

static void
_raidx_submit_null_payload_request(void *_raid_io)
{
	struct raid_bdev_io *raid_io = _raid_io;

	raidx_submit_null_payload_request(raid_io);
}

/*
 * brief:
 * raidx_submit_null_payload_request function submits the next batch of
 * io requests with range but without payload, like FLUSH and UNMAP, to member disks;
 * it will submit as many as possible unless one base io request fails with -ENOMEM,
 * in which case it will queue itself for later submission.
 * params:
 * bdev_io - pointer to parent bdev_io on raid bdev device
 * returns:
 * none
 */
static void
raidx_submit_null_payload_request(struct raid_bdev_io *raid_io)
{
	struct spdk_bdev_io		*bdev_io;
	struct raid_bdev		*raid_bdev;
	int				ret, pd_idx = 0;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	bdev_io = spdk_bdev_io_from_ctx(raid_io);
	raid_bdev = raid_io->raid_bdev;

	/* To add tgt lookup.
	 */
	pd_idx = libsdc_get_tgt(raid_bdev, bdev_io->u.bdev.offset_blocks);

	base_info = &raid_bdev->base_bdev_info[pd_idx];
	base_ch = raid_io->raid_ch->base_channel[pd_idx];

	assert(base_info != NULL);
	assert(base_ch != NULL);
	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_UNMAP:
		ret = spdk_bdev_unmap_blocks(base_info->desc, base_ch,
				bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
				raidx_bdev_io_completion, raid_io);
		break;

	case SPDK_BDEV_IO_TYPE_FLUSH:
		ret = spdk_bdev_flush_blocks(base_info->desc, base_ch,
				bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
				raidx_bdev_io_completion, raid_io);
		break;

	default:
		SPDK_ERRLOG("submit request, invalid io type with null payload %u\n", bdev_io->type);
		assert(false);
		ret = -EIO;
	}
	SPDK_DEBUGLOG("offset %lu, num_blocks %lu, type %d, pd_index %d, ret %d\n",
			bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, bdev_io->type, pd_idx, ret);

	if (ret == -ENOMEM) {
		raid_bdev_queue_io_wait(raid_io, base_info->bdev, base_ch,
				_raidx_submit_null_payload_request);
		return;
	} else if (ret != 0) {
		SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
		assert(false);
		raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}
}

static void raidx_stop(struct raid_bdev *raid_bdev)
{
	if (raid_bdev->module_private!= NULL) {
		free(raid_bdev->module_private);
		raid_bdev->module_private = NULL;
	}
	return;
}

static int raidx_start(struct raid_bdev *raid_bdev)
{
	uint64_t min_blockcnt = UINT64_MAX;
	struct raid_base_bdev_info *base_info;
    Ini_VolHandle *p_handle = calloc(1, sizeof(Ini_VolHandle));
    char str[17], str2[SPDK_UUID_STRING_LEN];
    uint64_t tsc = 0, tsc1=0, latency = 0;
    PLibSdc_Vol pFirst = NULL, pLast = NULL;

    const struct spdk_uuid * p_uuid = spdk_bdev_get_uuid(raid_bdev->base_bdev_info[0].bdev);
    spdk_uuid_fmt_lower(str2, sizeof(str2), p_uuid);
  	SPDK_NOTICELOG ("base dev uuid %s\n", str2);

    raid_bdev->module_private = (void *)p_handle;

	if (libsdc_init(raid_bdev) != 0 ) {
		return 1;
	}
	pLast = LibSdc_GetVolumesLast();
    for (pFirst = LibSdc_GetVolumesFirst(); pFirst != pLast; pFirst++) {
        // get which volume, and which offset
        uint8_t tgt = 0;

        LibSdc_VolIdToString(pFirst->volId, str);

        tsc = spdk_get_ticks();
        tgt = libsdc_get_tgt(raid_bdev, 0);
        tsc1= spdk_get_ticks();
        latency = (tsc1 - tsc)/200;

      	SPDK_NOTICELOG ("uuid %s vol id %s, init tgt %d, latency %lu us\n", str2, str, tgt, latency);

        if (memcmp(&str[4], &str2[24], 12) == 0) {
        	memcpy(raid_bdev->module_private, &pFirst->handle, sizeof (Ini_VolHandle));
        	break;
        }
    }
	/*
	 * Take the minimum block count based approach where total block count
	 * of raid bdev is the number of base bdev times the minimum block count
	 * of any base bdev.
	 */
	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		/* Calculate minimum block count from all base bdevs */
		min_blockcnt = spdk_min(min_blockcnt, base_info->bdev->blockcnt);
		SPDK_NOTICELOG("vol %s blockcount %lu, block_len %u\n",
				base_info->bdev->name, base_info->bdev->blockcnt, base_info->bdev->blocklen);
	}
	SPDK_DEBUGLOG(bdev_raid0, "min blockcount %" PRIu64 ",  numbasedev %u, strip size shift %u\n",
		      min_blockcnt, raid_bdev->num_base_bdevs, raid_bdev->strip_size_shift);

	raid_bdev->bdev.blockcnt = min_blockcnt;

	raid_bdev->bdev.optimal_io_boundary = 0;
	raid_bdev->bdev.split_on_optimal_io_boundary = false;

	return 0;
}

static struct raid_bdev_module g_raidx_module = {
	.level = RAIDX,
	.base_bdevs_min = 1,
	.start = raidx_start,
	.stop = raidx_stop,
	.submit_rw_request = raidx_submit_rw_request,
	.submit_null_payload_request = raidx_submit_null_payload_request,
};
RAID_MODULE_REGISTER(&g_raidx_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_raidx)
