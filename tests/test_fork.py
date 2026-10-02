"""Test that PooledDB and PersistentDB handle process boundaries.

When a process using a connection pool is forked, the child process
inherits the pool state, including locks that may be held by threads
that do not exist in the child and connections that are shared with
the parent process.  The pools must detect this situation and let the
child process build up its own pool state with its own connections,
without touching the inherited connections of the parent process.
"""

import ast
import itertools
import os
import select
import signal
import threading
import traceback

import pytest

from dbutils.persistent_db import PersistentDB
from dbutils.pooled_db import InvalidConnectionError, PooledDB

requires_fork = pytest.mark.skipif(
    not hasattr(os, 'fork'), reason='fork() is not available on this platform')


class Error(Exception):
    """Mock DB-API 2 base error."""


class OperationalError(Error):
    """Mock DB-API 2 operational error."""


class InterfaceError(Error):
    """Mock DB-API 2 interface error."""


class InternalError(Error):
    """Mock DB-API 2 internal error."""


class MockConnection:
    """Mock DB-API 2 connection recording the pid and a serial number."""

    OperationalError = OperationalError
    InterfaceError = InterfaceError
    InternalError = InternalError
    threadsafety = 1

    counter = itertools.count(1)

    def __init__(self):
        self.pid = os.getpid()
        self.number = next(MockConnection.counter)
        self.session = []
        self.closed = False

    def close(self):
        self.closed = True

    def commit(self):
        self.session.append('commit')

    def rollback(self):
        self.session.append('rollback')

    def cursor(self):
        return MockCursor(self)


class MockCursor:
    """Mock DB-API 2 cursor recording the executed statements."""

    def __init__(self, con):
        self._con = con

    def execute(self, operation):
        self._con.session.append(operation)

    def close(self):
        pass


def creator():
    """Creator function returning mock connections."""
    return MockConnection()


def shared_creator():
    """Creator function for connections that can be shared."""
    return MockConnection()


shared_creator.threadsafety = 2


def run_in_child(func, timeout=10):
    """Run the given function in a child process and return its result.

    The child process writes its result to a pipe and exits immediately
    with os._exit(), so that no cleanup code (finalizers, atexit hooks)
    can interfere with the parent process.  If the child process does
    not finish in time, it is killed and an error is raised, since this
    means the child deadlocked, e.g. on an inherited lock.
    """
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # child process
        try:
            os.close(read_fd)
            try:
                result = 'ok', func()
            except BaseException:
                result = 'error', traceback.format_exc()
            os.write(write_fd, repr(result).encode('ascii'))
        finally:
            os._exit(0)
    # parent process
    os.close(write_fd)
    try:
        ready, _, _ = select.select([read_fd], [], [], timeout)
        if not ready:
            os.kill(pid, signal.SIGKILL)
            raise AssertionError(
                'child process did not finish in time (deadlock?)')
        chunks = []
        while True:
            chunk = os.read(read_fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        os.close(read_fd)
        _, status = os.waitpid(pid, 0)
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
    outcome, result = ast.literal_eval(b''.join(chunks).decode('ascii'))
    if outcome == 'error':
        raise AssertionError(f'error in child process:\n{result}')
    return result


@requires_fork
def test_pooled_db_fork():
    """Check that a forked child process gets its own pool state."""
    pool = PooledDB(creator, mincached=1, maxcached=2,
                    setsession=['set datestyle to german'], ping=0)
    parent_pid = os.getpid()
    parent_con = pool.connection()
    parent_mock = parent_con.dbapi_connection
    assert parent_mock.pid == parent_pid
    parent_session = list(parent_mock.session)

    def use_pool_in_child():
        # the first use of the pool in the child must reinitialize it
        child_con = pool.connection()
        child_mock = child_con.dbapi_connection
        # using the connection borrowed by the parent must be rejected
        rejected = close_rejected = False
        try:
            parent_con.cursor()
        except InvalidConnectionError:
            rejected = True
        try:
            parent_con.close()
        except InvalidConnectionError:
            close_rejected = True
        result = (child_mock.pid, child_mock.number,
                  rejected, close_rejected, list(child_mock.session))
        child_con.close()
        return result

    (child_pid, child_number, rejected, close_rejected,
     child_session) = run_in_child(use_pool_in_child)
    # the child got its own connection, created in the child process
    assert child_pid != parent_pid
    assert child_number != parent_mock.number
    # the new connection uses the same session initialization
    assert 'set datestyle to german' in child_session
    # the connection borrowed by the parent was rejected in the child
    assert rejected
    assert close_rejected
    # the inherited parent connection was not closed or rolled back
    assert not parent_mock.closed
    assert parent_mock.session == parent_session
    # the pool in the parent process is still fully functional
    parent_con.close()
    con = pool.connection()
    assert con.dbapi_connection is parent_mock
    con.close()
    pool.close()


@requires_fork
def test_pooled_db_fork_shared_connection():
    """Check that shared connections are also rejected after fork()."""
    pool = PooledDB(shared_creator, mincached=1, maxshared=3, ping=0)
    parent_con = pool.connection()
    parent_mock = parent_con.dbapi_connection
    parent_pid = os.getpid()

    def use_pool_in_child():
        child_con = pool.connection()
        child_mock = child_con.dbapi_connection
        rejected = close_rejected = False
        try:
            parent_con.cursor()
        except InvalidConnectionError:
            rejected = True
        try:
            parent_con.close()
        except InvalidConnectionError:
            close_rejected = True
        result = (child_mock.pid, child_mock.number,
                  rejected, close_rejected)
        child_con.close()
        return result

    child_pid, child_number, rejected, close_rejected = run_in_child(
        use_pool_in_child)
    assert child_pid != parent_pid
    assert child_number != parent_mock.number
    assert rejected
    assert close_rejected
    assert not parent_mock.closed
    parent_con.close()
    pool.close()


@requires_fork
@pytest.mark.filterwarnings(
    'ignore:This process .* is multi-threaded:DeprecationWarning')
def test_pooled_db_fork_with_locked_pool():
    """Check that a forked child does not deadlock on the pool lock.

    When the pool lock is held by another thread while the process is
    forked, the child process inherits the locked lock, but the thread
    that would release it does not exist in the child.  The child must
    reinitialize the pool state instead of waiting for the lock.
    """
    pool = PooledDB(creator, mincached=1, ping=0)
    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def hold_pool_lock():
        with pool._lock:
            lock_acquired.set()
            release_lock.wait(10)

    thread = threading.Thread(target=hold_pool_lock)
    thread.start()
    try:
        assert lock_acquired.wait(10)

        def use_pool_in_child():
            # this would deadlock if the child used the inherited lock
            con = pool.connection()
            mock = con.dbapi_connection
            result = mock.pid, mock.number
            con.close()
            return result

        child_pid, child_number = run_in_child(use_pool_in_child)
        assert child_pid != os.getpid()
        # the child created its own fresh connection
        assert child_number > 1
    finally:
        release_lock.set()
        thread.join(10)
    pool.close()


@requires_fork
def test_persistent_db_fork():
    """Check that a forked child process gets its own connection."""
    persist = PersistentDB(
        creator, setsession=['set time zone to utc'], ping=0)
    parent_pid = os.getpid()
    parent_con = persist.connection()
    parent_mock = parent_con.dbapi_connection
    assert parent_mock.pid == parent_pid

    def use_persist_in_child():
        child_con = persist.connection()
        child_mock = child_con.dbapi_connection
        return child_mock.pid, child_mock.number, list(child_mock.session)

    child_pid, child_number, child_session = run_in_child(
        use_persist_in_child)
    # the child got its own connection, created in the child process
    assert child_pid != parent_pid
    assert child_number != parent_mock.number
    # the new connection uses the same session initialization
    assert 'set time zone to utc' in child_session
    # the inherited parent connection was not closed or rolled back
    assert not parent_mock.closed
    assert 'rollback' not in parent_mock.session
    # the parent process still uses its own persistent connection
    assert persist.connection().dbapi_connection is parent_mock


def test_no_fork_platform(monkeypatch):
    """Check that pools work normally when fork() is not available."""
    monkeypatch.delattr(os, 'fork')
    pool = PooledDB(creator, mincached=1, ping=0)
    assert pool._pid is None
    con = pool.connection()
    con.close()
    pool.close()
    persist = PersistentDB(creator, ping=0)
    assert persist._pid is None
    persist.connection()
