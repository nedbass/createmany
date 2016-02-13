/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this program; If not, see
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>
#include <pthread.h>

typedef struct {
        int     do_open;
        int     do_link;
        int     do_mkdir;
        int     do_unlink;
        int     do_mknod;
        char    *thread_dir;
        char    *fmt;
        char    *fmt_unlink;
        char    *tgt;
        long    begin;
        long    end;
        long    count;
        long    chunk;
        long    completed;
        int     interval;
        int     has_fmt_spec;
        int     unlink_has_fmt_spec;
        int     threads_done;
        pthread_mutex_t lock;
        pthread_cond_t writer_cv;
        pthread_cond_t master_cv;
} opts_struct_t;

static void usage(char *prog)
{
        printf("usage: %s {-o|-m|-d|-l<tgt>} [-t nthreads] [-s log] [-i interval] [-r altpath ] filenamefmt count\n", prog);
        printf("       %s {-o|-m|-d|-l<tgt>} [-t nthreads] [-s log] [-i interval] [-r altpath ] -D dir start count\n", prog);
        printf("       %s {-o|-m|-d|-l<tgt>} [-t nthreads] [-s log] [-i interval] [-r altpath ] filenamefmt ] -seconds\n", prog);
        printf("       %s {-o|-m|-d|-l<tgt>} [-t nthreads] [-s log] [-i interval] [-r altpath ] filenamefmt start count\n", prog);
        exit(EXIT_FAILURE);
}

static void get_file_name(const char *fmt, long n, pthread_t tid,
                           int has_fmt_spec, char *buf, char *thread_dir)
{
        int bytes;

        if (thread_dir)
                bytes = snprintf(buf, 4095, "%s/%u/%ld", thread_dir,
                                 (unsigned int)tid, n);
        else if (has_fmt_spec)
                bytes = snprintf(buf, 4095, fmt, n, (unsigned int)tid);
        else
                bytes = snprintf(buf, 4095, "%s%ld.%u", fmt, n,
                                 (unsigned int)tid);
        if (bytes >= 4095) {
                printf("file name too long\n");
                exit(EXIT_FAILURE);
        }
}

double now(void)
{
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return (double)tv.tv_sec + (double)tv.tv_usec / 1000000;
}

void * thr_fn(void *arg)
{
        char filename[4096];
        long i;
        int rc = 0;
        opts_struct_t *o = (opts_struct_t *)arg;
        long begin = o->begin;
        pthread_t tid;

        tid = pthread_self();

        if (o->thread_dir) {
                snprintf(filename, 4095, "%s/%u", o->thread_dir,
                         (unsigned int)tid);
                if (mkdir(filename, 0777) < 0) {
                        fprintf(stderr, "mkdir(%s) error: %s\n",
                                filename, strerror(errno));
                        exit(1);
                }
        }

        for (i = 0; i < o->chunk && time(NULL) < o->end; i++, begin++) {
                get_file_name(o->fmt, begin, tid, o->has_fmt_spec, filename,
                              o->thread_dir);
                if (o->do_open) {
                        int fd = open(filename, O_CREAT|O_RDWR, 0644);
                        if (fd < 0) {
                                printf("open(%s) error: %s\n", filename,
                                       strerror(errno));
                                rc = errno;
                                break;
                        }
                        close(fd);
                } else if (o->do_link) {
                        rc = link(o->tgt, filename);
                        if (rc) {
                                printf("link(%s, %s) error: %s\n",
                                       o->tgt, filename, strerror(errno));
                                rc = errno;
                                break;
                        }
                } else if (o->do_mkdir) {
                        rc = mkdir(filename, 0755);
                        if (rc) {
                                printf("mkdir(%s) error: %s\n",
                                       filename, strerror(errno));
                                rc = errno;
                                break;
                        }
                } else {
                        rc = mknod(filename, S_IFREG| 0444, 0);
                        if (rc) {
                                printf("mknod(%s) error: %s\n",
                                       filename, strerror(errno));
                                rc = errno;
                                break;
                        }
                }
                if (o->do_unlink) {
                        get_file_name(o->fmt_unlink, begin, tid,
                                      o->unlink_has_fmt_spec, filename,
                                      o->thread_dir);
                        rc = o->do_mkdir ? rmdir(filename) : unlink(filename);
                        if (rc) {
                                printf("unlink(%s) error: %s\n",
                                       filename, strerror(errno));
                                rc = errno;
                                break;
                        }
                }

                if (o->interval && (i + 1) % o->interval == 0) {
                        /* block here until main thread wakes me */
                        pthread_mutex_lock(&o->lock);
                        o->threads_done++;
                        o->completed += o->interval;
                        pthread_cond_signal(&o->master_cv);
                        pthread_cond_wait(&o->writer_cv, &o->lock);
                        pthread_mutex_unlock(&o->lock);
                }
        }
        pthread_mutex_lock(&o->lock);
        if (o->interval == 0) {
                o->completed += o->chunk;
                o->threads_done++;
        }
        pthread_cond_signal(&o->master_cv);
        pthread_mutex_unlock(&o->lock);

        return (void *)0; // FIXME return something meaningful
}

void init_opts(opts_struct_t *o)
{
        o->threads_done = 0;
        o->completed = 0;
        o->do_open = 0;
        o->do_link = 0;
        o->do_mkdir = 0;
        o->thread_dir = NULL;
        o->do_unlink = 0;
        o->do_mknod = 0;
        o->fmt = NULL;
        o->fmt_unlink = NULL;
        o->tgt = NULL;
        o->begin = 0;
        o->end = ~0UL >> 1;
        o->count = ~0UL >> 1;
        o->interval = 0;
        o->has_fmt_spec = 0;
        o->unlink_has_fmt_spec = 0;
}

int main(int argc, char ** argv)
{
        opts_struct_t opts;
        int rc = 0;
        FILE *log = stdout;
        int i;
        int c;
        int nthreads = 1;
        void *tret;
        pthread_t *ntid;
        double  start;
        double  last;
        long newcount;

        init_opts(&opts);
        start = last = now();

        /* Handle the last argument in form of "-seconds" */
        if (argc > 1 && argv[argc - 1][0] == '-') {
                char *endp;

                argc--;
                opts.end = strtol(argv[argc] + 1, &endp, 0);
                if (opts.end <= 0 || *endp != '\0')
                        usage(argv[0]);
                opts.end = opts.end + time(NULL);
        }

        while ((c = getopt(argc, argv, "omdD:i:l:r:s:t:")) != -1) {
                switch(c) {
                case 'i':
                        if ((opts.interval = atoi(optarg)) == 0) {
                                printf("invalid argument to -c: %s\n", optarg);
                                usage(argv[0]);
                        }
                        break;
                case 'o':
                        opts.do_open++;
                        break;
                case 'm':
                        opts.do_mknod++;
                        break;
                case 'd':
                        opts.do_mkdir++;
                        break;
                case 'D':
                        opts.thread_dir = optarg;
                        if (access(opts.thread_dir, W_OK|X_OK) < 0) {
                                fprintf(stderr, "error accessing directory %s: %s\n",
                                        opts.thread_dir, strerror(errno));
                                usage(argv[0]);
                        }
                        break;
                case 'l':
                        opts.do_link++;
                        opts.tgt = optarg;
                        break;
                case 'r':
                        opts.do_unlink++;
                        opts.fmt_unlink = optarg;
                        break;
                case 's':
                        if ((log = fopen(optarg, "w")) == NULL) {
                                printf("error opening %s for writing: %s\n",
                                        optarg, strerror(errno));
                                exit (1);
                        }
                        break;
                case 't':
                        if ((nthreads = atoi(optarg)) == 0) {
                                printf("invalid argument to -t: %s\n", optarg);
                                usage(argv[0]);
                        }
                        break;
                case '?':
                        printf("Unknown option '%c'\n", optopt);
                        usage(argv[0]);
                }
        }

        if (opts.do_open + opts.do_mkdir + opts.do_link + opts.do_mknod != 1 ||
            opts.do_unlink > 1)
                usage(argv[0]);

        if (!opts.thread_dir)
                opts.fmt = argv[optind++];

        switch (argc - optind) {
        case 2:
                opts.begin = strtol(argv[argc - 2], NULL, 0);
                /* fall through */
        case 1:
                if (opts.end != ~0UL >> 1) /* -seconds form */
                        usage(argv[0]);
                opts.count = strtol(argv[argc - 1], NULL, 0);
                break;
        default:
                usage(argv[0]);
        }

        if (opts.interval && opts.interval < nthreads) {
                fprintf(stderr, "Error: interval %d less than thread count %d.\n",
                        opts.interval, nthreads);
                usage(argv[0]);
        }

        if (opts.interval &&  opts.interval % nthreads != 0) {
                int newinterval = opts.interval - (opts.interval % nthreads);
                fprintf(stderr,
                        "Warning: thread count %d does not divide interval %d.\n",
                        nthreads, opts.interval);
                fprintf(stderr, "Warning: truncating interval to %d.\n",
                        newinterval);
                opts.interval = newinterval;
        }

        opts.interval = opts.interval / nthreads;

        if (opts.interval > opts.count) {
                fprintf(stderr, "Error: interval %d greater than count %lu.\n",
                        opts.interval, opts.count);
                usage(argv[0]);
        }

        if (opts.interval && opts.count % opts.interval != 0) {
                newcount = opts.count - (opts.count % opts.interval);
                fprintf(stderr,
                        "Warning: interval %d does not divide count %lu.\n",
                        opts.interval, opts.count);
                fprintf(stderr, "Warning: truncating count to %lu.\n",
                        newcount);
                opts.count = newcount;
        }

        if (opts.count % nthreads != 0) {
                newcount = opts.count - (opts.count % nthreads);
                fprintf(stderr,
                        "Warning: thread count %d does not divide count %lu.\n",
                        nthreads, opts.count);
                fprintf(stderr, "Warning: truncating count to %lu.\n",
                        newcount);
                opts.count = newcount;
        }

        opts.chunk = opts.count / nthreads;

        opts.has_fmt_spec = !opts.thread_dir && strchr(opts.fmt, '%') != NULL; // FIXME need two specs for n and tid
        if (opts.do_unlink)
                opts.unlink_has_fmt_spec = strchr(opts.fmt_unlink, '%') != NULL;

        ntid = malloc(sizeof(pthread_t) * nthreads);
        if (ntid == NULL) {
                fprintf(stderr, "Memory allocation failed, aborting.\n");
                return errno;
        }

        pthread_mutex_init(&opts.lock, NULL);
        pthread_cond_init(&opts.writer_cv, NULL);
        pthread_cond_init(&opts.master_cv, NULL);
        pthread_mutex_lock(&opts.lock);
        for (i=0; i < nthreads; i++)
                (void) pthread_create(&ntid[i], NULL, thr_fn, &opts); // FIXME check err

        while (opts.completed < opts.count) { // FIXME creates may fail
                while (opts.threads_done < nthreads)
                        pthread_cond_wait(&opts.master_cv, &opts.lock);

                if (opts.interval > 0) {
                        fprintf(log, "%ld %d\n", opts.completed,
                                (int)(opts.interval * nthreads/(now() - last)));
                        fflush(log);
                        last = now();
                        opts.threads_done = 0;
                        pthread_cond_broadcast(&opts.writer_cv);
                }
        }
        pthread_mutex_unlock(&opts.lock);

        fprintf(log, "total: %ld creates%s in %.6f seconds: %.6f creates/second\n",
                opts.count, opts.do_unlink ? "/deletions" : "",
                now() - start, ((double)opts.count / (now() - start)));

        for (i=0; i < nthreads; i++)
                (void) pthread_join(ntid[i], &tret); // FIXME check err

        free(ntid);
        return rc; // FIXME return something meaningful
}
