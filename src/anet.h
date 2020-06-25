/* anet.c -- Basic TCP socket stuff made a bit less boring
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef ANET_H
#define ANET_H

#define ANET_OK 0
#define ANET_ERR -1
#define ANET_ERR_LEN 256

/* Flags used with certain functions. */
#define ANET_NONE 0
#define ANET_IP_ONLY (1<<0)

#if defined(__sun)
#define AF_LOCAL AF_UNIX
#endif

int anetTcpConnect(char *err, char *addr, int port);
//{anetTcpGenericConnect(){socket();connect();}}
int anetTcpNonBlockConnect(char *err, char *addr, int port);
//{anetTcpGenericConnect(){socket();anetNonBlock();connect();}}
int anetTcpNonBlockBindConnect(char *err, char *addr, int port, char *source_addr);
//{anetTcpGenericConnect(){socket();anetNonBlock();bind();connect();}}
int anetUnixConnect(char *err, char *path);
//{anetUnixGenericConnect(){anetCreateSocket(){socket()};connect();};}
int anetUnixNonBlockConnect(char *err, char *path);
//{anetUnixGenericConnect(){anetCreateSocket(){socket()};anetNonBlock();connect()}}
int anetRead(int fd, char *buf, int count);
//{read()}
int anetResolve(char *err, char *host, char *ipbuf, size_t ipbuf_len);
//{anetGenericResolve()}
int anetResolveIP(char *err, char *host, char *ipbuf, size_t ipbuf_len);
//{anetGenericResolve()}
int anetTcpServer(char *err, int port, char *bindaddr, int backlog);
//{_anetTcpServer(){socket();anetListen(){bind();listen();}}}
int anetTcp6Server(char *err, int port, char *bindaddr, int backlog);
//{_anetTcpServer(){socket();anetListen(){bind();listen();}}}
int anetUnixServer(char *err, char *path, mode_t perm, int backlog);
//{_anetTcpServer(){socket();anetListen(){bind();listen();}}}
int anetTcpAccept(char *err, int serversock, char *ip, size_t ip_len, int *port);
//{anetGenericAccept(){accept();}}
int anetUnixAccept(char *err, int serversock);
//{anetGenericAccept(){accept();}}
int anetWrite(int fd, char *buf, int count);
//{write()}
int anetNonBlock(char *err, int fd);//
int anetEnableTcpNoDelay(char *err, int fd);
//{anetSetTcpNoDelay(){setsockopt()}
int anetDisableTcpNoDelay(char *err, int fd);
//{anetSetTcpNoDelay(){setsockopt()}
int anetTcpKeepAlive(char *err, int fd);
//{setsockopt()}
int anetPeerToString(int fd, char *ip, size_t ip_len, int *port);
//{getpeername()}
int anetKeepAlive(char *err, int fd, int interval);
//{setsockopt()}
int anetSockName(int fd, char *ip, size_t ip_len, int *port);
//{getsockname()}

#endif
