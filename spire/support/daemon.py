import errno
import os

from spire.core import Unit

class Daemon(Unit):
    """A daemon process."""

    def run(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

def detach_process():
    if os.fork():
        os._exit(0)
    os.setsid()
    if os.fork():
        os._exit(0)

    null = os.open(os.devnull, os.O_RDWR)
    try:
        for i in range(3):
            try:
                os.dup2(null, i)
            except OSError, error:
                if error.errno != errno.EBADF:
                    raise
    finally:
        os.close(null)

class Pidfile(object):
    def __init__(self, pidfile):
        self.pidfile = pidfile

    def read(self):
        try:
            with open(self.pidfile, 'r') as openfile:
                return int(openfile.read().strip())
        except (ValueError, IOError):
            return None

    def remove(self):
        try:
            os.unlink(pidfile)
        except OSError:
            with open(self.pidfile, 'w') as openfile:
                openfile.write('')

    def write(self, pid=None):
        pid = pid or os.getpid()
        with open(self.pidfile, 'w') as openfile:
            openfile.write(str(pid))

def switch_user(uid, gid=None):
    import grp, pwd

    try:
        uid = int(uid)
    except ValueError:
        func = pwd.getpwnam
    else:
        func = pwd.getpwuid

    try:
        user = func(uid)
    except KeyError:
        raise RuntimeError('invalid uid')

    if not gid:
        gid = user.pw_gid

    try:
        gid = int(gid)
    except ValueError:
        func = grp.getgrnam
    else:
        func = grp.getgrgid

    try:
        group = func(gid)
    except KeyError:
        raise RuntimeError('invalid gid')

    if group.gr_gid != os.getgid():
        os.setgid(group.gr_gid)
    if user.pw_uid == os.getuid():
        return

    groups = [group.gr_gid]
    maxgroups = os.sysconf('SC_NGROUPS_MAX')
    for group, password, lgid, users in grp.getgrall():
        if user.pw_name in users:
            groups.append(lgid)
            if len(groups) >= maxgroups:
                break

    unset = True
    while unset:
        try:
            os.setgroups(groups)
        except ValueError:
            if len(groups) > 1:
                del groups[-1]
            else:
                raise RuntimeError('failed to set group access list')
        except OSError, exception:
            if exception.args[0] == errno.EINVAL and len(groups) > 1:
                del groups[-1]
            else:
                raise RuntimeError('failed to set group access list')
        else:
            unset = False
    else:
        os.setuid(user.pw_uid)
