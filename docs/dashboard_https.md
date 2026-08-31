# Putting the dashboard behind HTTPS

The dashboard reads three live trading accounts and will place orders. It should
be reachable by Rahul and by nothing else on the internet.

**Tailscale Serve does that.** The host already runs Tailscale, the certificate
is real and automatic, and the service is never exposed publicly — no port
opened, no firewall rule, nothing to find by scanning. Caddy with a public domain
is the alternative and is documented at the end, but it puts an order-placing
service on the open internet behind one password, which is a worse trade for a
single user.

```
browser ─ tailnet ─▶ tailscale serve (TLS) ─▶ 127.0.0.1:8000 ─▶ agents ─▶ Fyers
                          ts.net cert          dashboard.service     loopback
```

The API binds **127.0.0.1 and nothing else**, in the unit itself. That is the
property worth protecting: a service that can place real orders must not be one
firewall rule away from being public.

## Setting it up

### 1. Build the UI and install the service

```bash
cd /root/trading_bot
cd webapp/web && npm install && npm run build && cd -

python3 deploy/gen_systemd_units.py
cp deploy/systemd/generated/dashboard.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now dashboard
systemctl status dashboard --no-pager
```

Unlike the bot units, this one **is** meant to be enabled — it should come back
after a reboot.

Check it locally before exposing it:

```bash
curl -s localhost:8000/api/health | python3 -m json.tool
```

### 2. Enable HTTPS on the tailnet

Once, in the Tailscale admin console (https://login.tailscale.com/admin/dns):
enable **MagicDNS**, then **HTTPS Certificates**. Without both, `tailscale serve`
cannot get a certificate.

### 3. Serve it

```bash
tailscale serve --bg 8000
tailscale serve status
```

That prints the URL — `https://trading.<your-tailnet>.ts.net/`. It works from any
device signed into the tailnet, with a valid certificate and no warning.

`--bg` persists across reboots. To stop: `tailscale serve --https=443 off`.

**Do not use `tailscale funnel`.** Funnel publishes to the whole internet, which
is exactly what this setup exists to avoid.

### 4. Turn on secure cookies

Only now, with real TLS in front:

```bash
sed -i 's/^COOKIE_SECURE=.*/COOKIE_SECURE=true/' /root/trading_bot/webapp/dashboard.env
systemctl restart dashboard
```

Before this the session cookie is not marked `Secure`. After it, the browser
will refuse to send the cookie over plain http — which is the point, and also
means `http://localhost:8000` will no longer log you in. Use the tailnet URL.

## Checking it

```bash
deploy/preflight.sh
tailscale serve status
systemctl is-enabled dashboard   # expect: enabled
```

## If a public domain is genuinely wanted

Point the domain's A record at `64.227.135.117`, then run Caddy in front:

```
your.domain {
    reverse_proxy 127.0.0.1:8000
}
```

Caddy obtains and renews the certificate itself. This requires opening 80 and
443 in ufw, and at that point the only thing between the internet and an
order-placing service is one bcrypt password. If you take this route, add
Tailscale-only access at the Caddy level, or a second factor — one password is
not enough for this particular service.

## Why not the Streamlit approach

The old dashboard binds Streamlit to the Tailscale IP directly (`100.109.109.19:8501`),
which is private but **plain http** — the password crosses the tailnet in clear.
Tailscale encrypts the transport, so that is not as bad as it sounds, but a real
certificate costs nothing here and means the browser is not being taught to
accept an insecure login form.
