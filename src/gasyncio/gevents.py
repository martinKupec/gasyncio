# coding: utf-8
#
# A minimalistic asyncio event loop based on the GLib main event loop
#
# Copyright (C) 2021 Itaï BEN YAACOV
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA


from gi.repository import GLib

import asyncio
import selectors
import sys
import threading


if sys.platform == 'win32':
    _GLib_IOChannel_new_socket = GLib.IOChannel.win32_new_socket
    os_events = asyncio.windows_events
else:
    _GLib_IOChannel_new_socket = GLib.IOChannel.unix_new
    os_events = asyncio.unix_events


class GAsyncIOSelector(selectors._BaseSelectorImpl):
    def __init__(self):
        super().__init__()
        self._sources = {}
        self._io_channels = {}
        self._fileobj_preserve = set()
        self._hups = set()

    @staticmethod
    def _events_to_io_condition(events):
        return \
            (GLib.IOCondition.IN if events & selectors.EVENT_READ else GLib.IOCondition(0)) | \
            (GLib.IOCondition.OUT if events & selectors.EVENT_WRITE else GLib.IOCondition(0))

    @staticmethod
    def _io_condition_to_events(condition):
        return \
            (selectors.EVENT_READ if condition & GLib.IOCondition.IN else 0) | \
            (selectors.EVENT_WRITE if condition & GLib.IOCondition.OUT else 0)

    def register(self, fileobj, events, data):
        key = super().register(fileobj, events, data)
        if key.fd in self._sources:
            raise KeyError
        if key.fd in self._io_channels:
            io_channel = self._io_channels[key.fd]
        else:
            io_channel = _GLib_IOChannel_new_socket(key.fd)
            io_channel.set_encoding(None)
            io_channel.set_buffered(False)
            io_channel.set_close_on_unref(False)
            self._io_channels[key.fd] = io_channel
        self._sources[key.fd] = GLib.io_add_watch(io_channel, GLib.PRIORITY_DEFAULT, self._events_to_io_condition(events) | GLib.IOCondition.HUP, self._channel_watch_cb, key)
        return key

    def unregister(self, fileobj):
        key = super().unregister(fileobj)
        source = self._sources.pop(key.fd)
        GLib.source_remove(source)
        if not fileobj in self._fileobj_preserve:
            io_channel = self._io_channels[key.fd]
            del self._io_channels[key.fd]
            del io_channel
        return key

    def modify(self, fileobj, events, data=None):
        self._fileobj_preserve.add(fileobj)
        ret = super().modify(fileobj, events, data)
        self._fileobj_preserve.remove(fileobj)
        return ret

    def _channel_watch_cb(self, channel, condition, key):
        handle_in, handle_out = key.data
        if condition & GLib.IOCondition.IN:
            handle_in._run()
        if condition & GLib.IOCondition.OUT:
            handle_out._run()
        if condition & GLib.IOCondition.HUP:
            self._hups.add(key)
        return True

    def select(self, timeout=None):
        ready = []
        for key in self._hups:
            ready.append((key, key.events & (selectors.EVENT_READ | selectors.EVENT_WRITE)))
        self._hups.clear()
        return ready

class NoWaitSelectSelector(selectors.SelectSelector):
    _schedule_periodic_giterate = True

    def select(self, timeout=None):
        return super().select(0)

class GAsyncIOEventLoop(os_events.SelectorEventLoop):
    UserSelector = None

    def __init__(self, selector=None):
        self._is_slave = False
        self._giteration = None
        self._lock = threading.Lock()
        if selector is None:
            if GAsyncIOEventLoop.UserSelector is not None:
                selector = GAsyncIOEventLoop.UserSelector
            elif sys.platform == 'win32':
                selector = NoWaitSelectSelector()
            else:
                selector = GAsyncIOSelector()
        super().__init__(selector)

    def is_running(self):
        return self._is_slave

    def start_slave_loop(self):
        """
        Prepare loop to be run by the GLib main event loop.
        asyncio.get_event_loop() and asyncio.get_running_loop() will
        return self, but self.is_running() will return False.
        """
        self._check_closed()
        self._check_running()
        self._set_coroutine_origin_tracking(self._debug)

        self._old_agen_hooks = sys.get_asyncgen_hooks()
        sys.set_asyncgen_hooks(firstiter=self._asyncgen_firstiter_hook,
                               finalizer=self._asyncgen_finalizer_hook)
        asyncio.events._set_running_loop(self)
        self._is_slave = True

    def stop_slave_loop(self):
        """
        Undo the effects of self.start_slave_loop().
        """
        if not self._is_slave:
            raise RuntimeError('This event loop is not running as a slave')
        self._is_slave = False
        asyncio.events._set_running_loop(None)
        self._set_coroutine_origin_tracking(False)
        sys.set_asyncgen_hooks(*self._old_agen_hooks)

    def run_until_complete(self, future):
        future = asyncio.tasks.ensure_future(future, loop=self)
        gloop = GLib.MainLoop.new(None, False)

        def _run_until_complete_cb(task):
            gloop.quit()

        future.add_done_callback(_run_until_complete_cb)
        gloop.run()
        future.remove_done_callback(_run_until_complete_cb)
        future.exception()
        return future.result()

    def close(self):
        if self._giteration is not None:
            GLib.source_remove(self._giteration)
            self._giteration = None
        super().close()

    def call_at(self, when, callback, *args, context=None):
        self._check_closed()
        if self._debug:
            self._check_thread()
            self._check_callback(callback, 'call_at')
        timer = asyncio.events.TimerHandle(when, callback, args, self, context)
        if timer._source_traceback:
            del timer._source_traceback[-1]
        delay = (when - self.time()) * 1000
        if delay < 0:
            delay = 0
        timer._scheduled = GLib.timeout_add(delay, self._timeout_cb, timer)
        return timer

    def _timer_handle_cancelled(self, timer):
        if timer._scheduled:
            GLib.source_remove(timer._scheduled)
        timer._scheduled = False

    def _timeout_cb(self, timer):
        timer._run()
        timer._scheduled = False
        return False

    def _call_soon(self, *args, **kwargs):
        handle = super()._call_soon(*args, **kwargs)
        self._schedule_giteration()
        return handle

    def _add_callback(self, *args, **kwargs):
        handle = super()._add_callback(*args, **kwargs)
        self._schedule_giteration()
        return handle

    def _schedule_giteration(self):
        with self._lock:
            if self._giteration is None:
                self._giteration = GLib.timeout_add(0, self._giterate)

    def _giterate(self):
        self._run_once()
        if self._ready:
            return True
        with self._lock:
            if getattr(self._selector, '_schedule_periodic_giterate', False):
                self._giteration = GLib.timeout_add(10, self._giterate)
            else:
                self._giteration = None
        return False


class GAsyncIOEventLoopPolicy(os_events.DefaultEventLoopPolicy):
    _loop_factory = GAsyncIOEventLoop


def start_slave_loop(selector=None):
    GAsyncIOEventLoop.UserSelector = selector
    asyncio.set_event_loop_policy(GAsyncIOEventLoopPolicy())
    loop = asyncio.get_event_loop()
    loop.start_slave_loop()


def stop_slave_loop():
    loop = asyncio.get_event_loop()
    loop.stop_slave_loop()
    loop.close()
    asyncio.set_event_loop_policy(None)
