# Per-account proxy setup (static whitelisted IP)

Each non-home account reaches Fyers through a proxy whose **public IP is that account's
whitelisted static IP**. The master host connects to the proxy; the proxy makes the outbound
Fyers call, so Fyers sees the proxy box's IP.

- **Home account (Rahul / user1):** no proxy — the master host `64.227.135.117` *is* his
  whitelisted IP.
- **Pratibha (user2):** proxy on `157.245.108.24` (her existing Shishind VPS / whitelisted IP).
- Each additional user: one more proxy box holding that user's whitelisted IP.

Installing the proxy is **additive** — if the box currently runs that account's live bot, the
proxy runs alongside it and disturbs nothing. At cutover the bot logic moves to the master
host; the box stays purely as the proxy, keeping its whitelisted IP.

## Install tinyproxy (Debian/Ubuntu VPS)

Run on the proxy box (e.g. `157.245.108.24`):

```bash
sudo apt-get update && sudo apt-get install -y tinyproxy
```

Edit `/etc/tinyproxy/tinyproxy.conf`:

```
Port 3128
Listen 0.0.0.0

# Who may USE this proxy — ONLY the master host. Remove/curtail the default localhost allows
# if you don't need local testing. An open proxy WILL get abused, so keep this tight.
Allow 64.227.135.117

# Do not add Upstream — we want tinyproxy to egress directly from THIS box's public IP.
```

```bash
sudo systemctl enable --now tinyproxy
sudo systemctl restart tinyproxy
```

## Firewall — only the master host may reach 3128

```bash
sudo ufw allow from 64.227.135.117 to any port 3128 proto tcp
sudo ufw deny 3128           # block 3128 from everyone else
```

(If not using ufw, do the equivalent with your cloud provider's firewall / security group:
allow TCP 3128 inbound only from `64.227.135.117`.)

## Verify egress — FROM THE MASTER HOST (not your laptop)

The proxy is firewalled to the master host, so the test only passes from there:

```bash
# on 64.227.135.117:
HTTPS_PROXY=http://157.245.108.24:3128 curl -s https://api.ipify.org ; echo
# expected output: 157.245.108.24   (Pratibha's whitelisted IP)
```

If it prints `157.245.108.24`, the binding works and that IP must be whitelisted in
Pratibha's Fyers API app. `Connection refused` → tinyproxy not running or firewall/Allow is
blocking the master host.

## Hardened variant (optional) — keep 3128 off the public internet

If the proxy box is on your Tailscale net, point the account's `HTTPS_PROXY` at the proxy's
**Tailscale** IP and bind tinyproxy to the Tailscale interface. The master→proxy hop then
never touches the public internet; the proxy still egresses to Fyers from its public
whitelisted IP. Only outbound Fyers traffic uses the public IP; the proxy port isn't exposed.
