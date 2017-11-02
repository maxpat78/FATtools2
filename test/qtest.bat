@echo off
REM ~ mkfat.py g: -t fat32 && stress.py g: -p15 -t0.5 --sha1 && chkdsk g: && start G:\hashes.sha1
REM ~ mkfat.py g: -t exfat -c 32k && stress.py g: -t0.5 --sha1 && chkdsk g: && start G:\hashes.sha1
REM ~ mkfat.py g: -t exfat -c 256k && stress.py g: -p15 -t80 --sha1 && chkdsk g: && start G:\hashes.sha1
REM ~ mkfat.py g: -t exfat -c  32k && stress.py g: --fix -p15 -t19.3 --sha1 && chkdsk g: && start G:\hashes.sha1
mkfat.py g: -t exfat && stress.py g: -t90 --sha1 && chkdsk g: && start G:\hashes.sha1
REM ~ mkfat.py g: -t fat32 && stress.py g: -t 15 --sha1 --fix --debug 4 && chkdsk g:/f && start G:\hashes.sha1