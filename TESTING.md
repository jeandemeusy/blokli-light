# Testing blokli-light

Run all project commands inside `nix develop`.

```bash
just quick
just test
just test-postgres
```

`just quick` formats the tree, runs Clippy with warnings denied for all targets, and checks compilation. `just test` runs the unit suite.
`just test-postgres` starts an isolated PostgreSQL cluster and verifies migration compatibility, duplicate idempotency, reorg replacement,
the origin guard, and read-only role permissions.

The end-to-end acceptance test uses an Anvil chain: deploy the HOPR contracts, emit representative channel, announcement, and oracle events,
register a custom ERC-20 after it has emitted transfers, and verify both its historical backfill and live following.

Final parity is measured against the original Blokli over the same real-network range:

```sql
SELECT block_number, tx_index, log_index, address, topics, data
FROM log
ORDER BY 1, 2, 3;
```

The result must match byte-for-byte for the deployment-configured contract set. The Arrakis `gnosis-vpn.json` bundle must then produce the
same query results through the provisioned reader role.
