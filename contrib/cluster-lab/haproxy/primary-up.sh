#!/bin/bash
# Healthy only once HAProxy routes a primary: one pg-N line is UP in the
# stats CSV of the "primary" backend. No curl in the image, so /dev/tcp.
exec 3<>/dev/tcp/127.0.0.1/7000 || exit 1
printf 'GET /;csv HTTP/1.0\r\n\r\n' >&3
grep -qE '^primary,pg-[123],([^,]*,){15}UP,' <&3
