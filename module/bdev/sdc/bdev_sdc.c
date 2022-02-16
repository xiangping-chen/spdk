/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2022 DELL CORPORATION & AFFILIATES. All rights reserved.
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

#include "spdk/stdinc.h"

#include "bdev_sdc.h"
#include "spdk/bdev.h"
#include "spdk/endian.h"
#include "spdk/env.h"
#include "spdk/accel_engine.h"
#include "spdk/json.h"
#include "spdk/thread.h"
#include "spdk/queue.h"
#include "spdk/string.h"

#include "spdk/bdev_module.h"
#include "spdk/log.h"

#include "libsdc.h"

/* The disk configuration */
struct sdc_disk {
	struct spdk_bdev		disk;
	UINT64					sdc_handle;
	TAILQ_ENTRY(sdc_disk)	link;
};

/* device specific IO context */
typedef struct sdc_task{
	SimpleIoCtx 				libSdcCtx;
	int							num_outstanding;
	enum spdk_bdev_io_status	status;
	TAILQ_ENTRY(sdc_task)		tailq;
} sdc_task, *Psdc_task;

struct sdc_channel {
//	struct spdk_io_channel		*accel_channel;
	struct spdk_poller		*completion_poller;
	TAILQ_HEAD(, sdc_task)	completed_tasks;
};


#define START_OF_STRUCT(ptr, type, member)({\
    const typeof(((type *)0)->member)*__mptr = (ptr);\
    (type *)((char *)__mptr - offsetof(type, member)); })

/**
 * Get wrapper pointer from inner Io Ctx.
 * @param pIoCtx
 * @return
 */
static Psdc_task getWrapperFromIoCtx(PSimpleIoCtx pIoCtx) {
    return START_OF_STRUCT(pIoCtx, sdc_task, libSdcCtx);
}

static TAILQ_HEAD(, sdc_disk) g_sdc_disks = TAILQ_HEAD_INITIALIZER(g_sdc_disks);

int sdc_disk_count = 0;

static int bdev_sdc_initialize(void);
static void bdev_sdc_deinitialize(void);

static int
bdev_sdc_get_ctx_size(void)
{
	return sizeof(sdc_task);
}

static struct spdk_bdev_module sdc_if = {
	.name = "sdc",
	.module_init = bdev_sdc_initialize,
	.module_fini = bdev_sdc_deinitialize,
	.get_ctx_size = bdev_sdc_get_ctx_size,

};

SPDK_BDEV_MODULE_REGISTER(sdc, &sdc_if)

static void
sdc_disk_free(struct sdc_disk *sdc_disk)
{
	if (!sdc_disk) {
		return;
	}

	free(sdc_disk->disk.name);
	free(sdc_disk);
}

static int
bdev_sdc_destruct(void *ctx)
{
	struct sdc_disk *sdc_disk = ctx;

	TAILQ_REMOVE(&g_sdc_disks, sdc_disk, link);
	sdc_disk_free(sdc_disk);
	return 0;
}

static struct spdk_io_channel *
bdev_sdc_get_io_channel(void *ctx)
{
	return spdk_get_io_channel(&g_sdc_disks);
}

static void
sdc_complete_task(struct sdc_task *task, struct sdc_channel *mch,
		     enum spdk_bdev_io_status status)
{
	task->status = status;
	TAILQ_INSERT_TAIL(&mch->completed_tasks, task, tailq);
}

static int mosErrorToEnosys(Mos_RC rc) {
    switch (rc) {
        case 0:
        case LIBSDC_MOS_SUCCESS:
        	return SPDK_BDEV_IO_STATUS_SUCCESS;
        case LIBSDC_MOS_ASYNC_SUCCESS:
            return SPDK_BDEV_IO_STATUS_PENDING;
        default:
            return SPDK_BDEV_IO_STATUS_FAILED;
    }
}

static void
sdc_libsdc_io_done(PLibSdc_IoCtx _pCtx, Mos_RC error)
{
	Psdc_task p_task = getWrapperFromIoCtx(_pCtx);
	struct sdc_channel *ch = bdev_sdc_get_io_channel(&g_sdc_disks);

	enum spdk_bdev_io_status status = mosErrorToEnosys(error);

	if (--p_task->num_outstanding == 0) {
		sdc_complete_task(p_task, ch, status);
	}
}

static int
bdev_sdc_check_iov_len(struct iovec *iovs, int iovcnt, size_t nbytes)
{
	int i;

	for (i = 0; i < iovcnt; i++) {
		if (nbytes < iovs[i].iov_len) {
			return 0;
		}

		nbytes -= iovs[i].iov_len;
	}

	return nbytes != 0;
}

static void sdc_set_libsdc_io_sgl(struct sdc_task *ptask,
		struct iovec *iov, int iovcnt, size_t len, uint64_t offset, enum spdk_bdev_io_type ioType)
{
	PSimpleIoCtx myCtx = &(ptask->libSdcCtx);
    uint32_t i = 0;

    while(i < iovcnt)
    {
    	myCtx->sg[i].buf = iov[i].iov_base;
        myCtx->sg[i].sizeInLB = iov[i].iov_len/512;
    }
    myCtx->base.pBuffers = myCtx->sg;
    myCtx->base.numBuffers = iovcnt;

    switch(ioType) {
    case SPDK_BDEV_IO_TYPE_READ:
    	myCtx->ioType = SIMPLE_IO_READ;
    	break;
    case SPDK_BDEV_IO_TYPE_WRITE:
    	myCtx->ioType = SIMPLE_IO_WRITE;
    	break;
    default:
    	break;
    }

    return;
}


/*
 * Params:
 * mdisk: (struct sdc_disk *)bdev_io->bdev->ctxt,
 * task: (struct sdc_task *)bdev_io->driver_ctx,
 * iov: bdev_io->u.bdev.iovs,
 * iovcnt: bdev_io->u.bdev.iovcnt,
 * len: bdev_io->u.bdev.num_blocks * block_size,
 * offset: bdev_io->u.bdev.offset_blocks * block_size
 */
static void
bdev_sdc_readv(struct sdc_disk *mdisk,
		  struct sdc_task *task,
		  struct iovec *iov, int iovcnt, size_t len, uint64_t offset)
{
	uint64_t start_lb = offset / LB_SIZE_IN_BYTES;
    Mos_RC rc;

	if (bdev_sdc_check_iov_len(iov, iovcnt, len)) {
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	SPDK_DEBUGLOG(bdev_sdc, "read %zu bytes from offset %#" PRIx64 ", iovcnt=%d\n",
		      len, offset, iovcnt);

	task->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	task->num_outstanding = 1;
	sdc_set_libsdc_io_sgl(task, iov, iovcnt, len, offset, SPDK_BDEV_IO_TYPE_READ);

    rc = libsdcIO_Read(mdisk->sdc_handle, start_lb, LIBSDC_IO_FLAG__NONE, &(task->libSdcCtx.base));
    task->status = mosErrorToEnosys(rc);
    return;
}

static void
bdev_sdc_writev(struct sdc_disk *mdisk, struct sdc_task *task,
		   struct iovec *iov, int iovcnt, size_t len, uint64_t offset)
{
	uint64_t start_lb = offset / LB_SIZE_IN_BYTES;
    Mos_RC rc;

	if (bdev_sdc_check_iov_len(iov, iovcnt, len)) {
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	SPDK_DEBUGLOG(bdev_sdc, "wrote %zu bytes to offset %#" PRIx64 ", iovcnt=%d\n",
		      len, offset, iovcnt);

	task->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	task->num_outstanding = 1;

	sdc_set_libsdc_io_sgl(task, iov, iovcnt, len, offset, SPDK_BDEV_IO_TYPE_WRITE);

    rc = libsdcIO_Write(mdisk->sdc_handle, start_lb, LIBSDC_IO_FLAG__NONE, &(task->libSdcCtx.base));
    task->status = mosErrorToEnosys(rc);
    return;
}

static int
bdev_sdc_unmap(struct sdc_disk *mdisk,
		  struct sdc_task *task,
		  uint64_t offset,
		  uint64_t byte_count)
{
	sdc_complete_task(task, NULL, SPDK_BDEV_IO_STATUS_SUCCESS);

	return 0;
}

static int _bdev_sdc_submit_request(struct sdc_channel *mch, struct spdk_bdev_io *bdev_io)
{
	uint32_t block_size = bdev_io->bdev->blocklen;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		if (bdev_io->u.bdev.iovs[0].iov_base == NULL) {
			assert(bdev_io->u.bdev.iovcnt == 1);
			bdev_io->u.bdev.iovs[0].iov_base = NULL;
			bdev_io->u.bdev.iovs[0].iov_len = bdev_io->u.bdev.num_blocks * block_size;
			sdc_complete_task((struct sdc_task *)bdev_io->driver_ctx, mch,
					     SPDK_BDEV_IO_STATUS_SUCCESS);
			return 0;
		}

		bdev_sdc_readv((struct sdc_disk *)bdev_io->bdev->ctxt,
				  (struct sdc_task *)bdev_io->driver_ctx,
				  bdev_io->u.bdev.iovs,
				  bdev_io->u.bdev.iovcnt,
				  bdev_io->u.bdev.num_blocks * block_size,
				  bdev_io->u.bdev.offset_blocks * block_size);
		return 0;

	case SPDK_BDEV_IO_TYPE_WRITE:
		bdev_sdc_writev((struct sdc_disk *)bdev_io->bdev->ctxt,
				   (struct sdc_task *)bdev_io->driver_ctx,
				   bdev_io->u.bdev.iovs,
				   bdev_io->u.bdev.iovcnt,
				   bdev_io->u.bdev.num_blocks * block_size,
				   bdev_io->u.bdev.offset_blocks * block_size);
		return 0;

	case SPDK_BDEV_IO_TYPE_RESET:
		sdc_complete_task((struct sdc_task *)bdev_io->driver_ctx, mch,
				     SPDK_BDEV_IO_STATUS_SUCCESS);
		return 0;

	case SPDK_BDEV_IO_TYPE_FLUSH:
		sdc_complete_task((struct sdc_task *)bdev_io->driver_ctx, mch,
				     SPDK_BDEV_IO_STATUS_SUCCESS);
		return 0;

	case SPDK_BDEV_IO_TYPE_UNMAP:
		return bdev_sdc_unmap((struct sdc_disk *)bdev_io->bdev->ctxt,
					 (struct sdc_task *)bdev_io->driver_ctx,
					 bdev_io->u.bdev.offset_blocks * block_size,
					 bdev_io->u.bdev.num_blocks * block_size);

	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		/* bdev_sdc_unmap is implemented with a call to mem_cpy_fill which zeroes out all of the requested bytes. */
		return bdev_sdc_unmap((struct sdc_disk *)bdev_io->bdev->ctxt,
					 (struct sdc_task *)bdev_io->driver_ctx,
					 bdev_io->u.bdev.offset_blocks * block_size,
					 bdev_io->u.bdev.num_blocks * block_size);

	case SPDK_BDEV_IO_TYPE_ABORT:
		sdc_complete_task((struct sdc_task *)bdev_io->driver_ctx, mch,
				     SPDK_BDEV_IO_STATUS_FAILED);
		return 0;
	default:
		return -1;
	}
	return 0;
}

static void bdev_sdc_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct sdc_channel *mch = spdk_io_channel_get_ctx(ch);

	if (_bdev_sdc_submit_request(mch, bdev_io) != 0) {
		sdc_complete_task((struct sdc_task *)bdev_io->driver_ctx, mch,
				     SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static bool
bdev_sdc_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
		return true;
	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_RESET:
	case SPDK_BDEV_IO_TYPE_UNMAP:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
	case SPDK_BDEV_IO_TYPE_ZCOPY:
	case SPDK_BDEV_IO_TYPE_ABORT:

	default:
		return false;
	}
}

static void
bdev_sdc_write_json_config(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	char uuid_str[SPDK_UUID_STRING_LEN];

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "method", "bdev_sdc_create");

	spdk_json_write_named_object_begin(w, "params");
	spdk_json_write_named_string(w, "name", bdev->name);
	spdk_json_write_named_uint64(w, "num_blocks", bdev->blockcnt);
	spdk_json_write_named_uint32(w, "block_size", bdev->blocklen);
	spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), &bdev->uuid);
	spdk_json_write_named_string(w, "uuid", uuid_str);
	spdk_json_write_named_uint32(w, "optimal_io_boundary", bdev->optimal_io_boundary);

	spdk_json_write_object_end(w);

	spdk_json_write_object_end(w);
}

static const struct spdk_bdev_fn_table sdc_fn_table = {
	.destruct		= bdev_sdc_destruct,
	.submit_request		= bdev_sdc_submit_request,
	.io_type_supported	= bdev_sdc_io_type_supported,
	.get_io_channel		= bdev_sdc_get_io_channel,
	.write_config_json	= bdev_sdc_write_json_config,
};

int
create_sdc_disk(struct spdk_bdev **bdev, const char *name, const struct spdk_uuid *uuid)
{
	struct sdc_disk	*mdisk;
	int rc;
	bool bFound = false;
    PLibSdc_Vol pFirst = NULL, pLast = NULL;

	mdisk = calloc(1, sizeof(*mdisk));
	if (!mdisk) {
		SPDK_ERRLOG("mdisk calloc() failed\n");
		return -ENOMEM;
	}

	pLast = LibSdc_GetVolumesLast();
    for (pFirst = LibSdc_GetVolumesFirst(); pFirst != pLast; pFirst++) {
    	int volId = atoi(name);

        if (volId == pFirst->localId) {
        	memcpy(&mdisk->sdc_handle, &pFirst->handle, sizeof (Ini_VolHandle));

        	mdisk->disk.blocklen = LB_SIZE_IN_BYTES;
        	mdisk->disk.blockcnt = pFirst->sizeInBytes/LB_SIZE_IN_BYTES;
        	bFound = true;
        	break;
        }
    }
    if (!bFound) {
    	free(mdisk);
    	return -EINVAL;
    }

    if (name) {
		mdisk->disk.name = strdup(name);
	} else {
		/* Auto-generate a name */
		mdisk->disk.name = spdk_sprintf_alloc("sdc%d", sdc_disk_count);
	}
	sdc_disk_count++;
	if (!mdisk->disk.name) {
		sdc_disk_free(mdisk);
		return -ENOMEM;
	}
	mdisk->disk.product_name = "sdc disk";

	if (uuid) {
		mdisk->disk.uuid = *uuid;
	} else {
		spdk_uuid_generate(&mdisk->disk.uuid);
	}

	mdisk->disk.ctxt = mdisk;
	mdisk->disk.fn_table = &sdc_fn_table;
	mdisk->disk.module = &sdc_if;

	rc = spdk_bdev_register(&mdisk->disk);
	if (rc) {
		sdc_disk_free(mdisk);
		return rc;
	}

	*bdev = &(mdisk->disk);

	TAILQ_INSERT_TAIL(&g_sdc_disks, mdisk, link);

	return rc;
}

void
delete_sdc_disk(struct spdk_bdev *bdev, spdk_delete_sdc_complete cb_fn, void *cb_arg)
{
	if (!bdev || bdev->module != &sdc_if) {
		cb_fn(cb_arg, -ENODEV);
		return;
	}

	spdk_bdev_unregister(bdev, cb_fn, cb_arg);
}

static int
sdc_completion_poller(void *ctx)
{
	struct sdc_channel *ch = ctx;
	struct sdc_task *task;
	TAILQ_HEAD(, sdc_task) completed_tasks;
	uint32_t num_completions = 0;

	TAILQ_INIT(&completed_tasks);
	TAILQ_SWAP(&completed_tasks, &ch->completed_tasks, sdc_task, tailq);

	while (!TAILQ_EMPTY(&completed_tasks)) {
		task = TAILQ_FIRST(&completed_tasks);
		TAILQ_REMOVE(&completed_tasks, task, tailq);
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task), task->status);
		num_completions++;
	}

	return num_completions > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

static int
sdc_create_channel_cb(void *io_device, void *ctx)
{
	struct sdc_channel *ch = ctx;

	ch->completion_poller = SPDK_POLLER_REGISTER(sdc_completion_poller, ch, 0);
	if (!ch->completion_poller) {
		SPDK_ERRLOG("Failed to register sdc completion poller\n");
		return -ENOMEM;
	}

	TAILQ_INIT(&ch->completed_tasks);

	return 0;
}

static void
sdc_destroy_channel_cb(void *io_device, void *ctx)
{
	struct sdc_channel *ch = ctx;

	assert(TAILQ_EMPTY(&ch->completed_tasks));

//	spdk_put_io_channel(ch->accel_channel);
	spdk_poller_unregister(&ch->completion_poller);
}

static int sdc_libsdc_init()
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
    	mdmAddress.ipv4 = LIBSDC_GENERATE_IPV4_ADDRESS(10, 246, 67, 251);
    	mdmAddress.port = 6611;
    	LIBSDC_SO_CONFIG_SET_PARAM(&sdcConfig, guid, guid);
    	LIBSDC_SO_CONFIG_SET_PARAM(&sdcConfig, mdm, mdmAddress);

    	/*
    	 * BOOL LibSdc_InitSimple(
    	        PLibSdc_PrereqConfig pInitConfig,
    	        PLibSdc_Config pConfig,
    	        LibSdc_ReadCb readCb, LibSdc_WriteCb writeCb, LibSdc_ConnectCb connectCb) {
    	 *
    	 */
    	if (!LibSdc_InitSimple(NULL, &sdcConfig, sdc_libsdc_io_done, sdc_libsdc_io_done, NULL)) {
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


static int bdev_sdc_initialize(void)
{
    PLibSdc_Vol pFirst = NULL, pLast = NULL;

    /* This needs to be reset for each reinitialization of submodules.
	 * Otherwise after enough devices or reinitializations the value gets too high.
	 * TODO: Make sdc bdev name mandatory and remove this counter. */
	sdc_disk_count = 0;

	spdk_io_device_register(&g_sdc_disks, sdc_create_channel_cb,
				sdc_destroy_channel_cb, sizeof(struct sdc_channel),
				"bdev_sdc");

	if (sdc_libsdc_init() != 0 ) {
		return 1;
	}
	pLast = LibSdc_GetVolumesLast();
    for (pFirst = LibSdc_GetVolumesFirst(); pFirst != pLast; pFirst++) {
        char 	str[SPDK_UUID_STRING_LEN];
        LibSdc_VolIdToString(pFirst->volId, str);

      	SPDK_NOTICELOG ("vol %d id %s\n", pFirst->localId, str);
    }

	return 0;
}

static void
bdev_sdc_deinitialize(void)
{
	spdk_io_device_unregister(&g_sdc_disks, NULL);
}

SPDK_LOG_REGISTER_COMPONENT(bdev_sdc)
