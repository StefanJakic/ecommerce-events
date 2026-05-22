#!/bin/bash
# Salje 30 zahteva paralelno - 3 customer-a sa razlicitim ishodima

echo "Pocetak: $(date '+%H:%M:%S')"

for i in $(seq 1 10); do
  # 10x CONFIRMED (SKU-001 - ima 100 na zalihi)
  curl -s -X POST http://localhost:8080/orders -H "Content-Type: application/json" \
    -d "{\"customerId\":\"BULK-A$i\",\"items\":[{\"sku\":\"SKU-001\",\"quantity\":1}]}" \
    -o /dev/null &

  # 10x UNKNOWN_SKU
  curl -s -X POST http://localhost:8080/orders -H "Content-Type: application/json" \
    -d "{\"customerId\":\"BULK-B$i\",\"items\":[{\"sku\":\"NEMA\",\"quantity\":1}]}" \
    -o /dev/null &

  # 10x INSUFFICIENT_STOCK
  curl -s -X POST http://localhost:8080/orders -H "Content-Type: application/json" \
    -d "{\"customerId\":\"BULK-C$i\",\"items\":[{\"sku\":\"SKU-042\",\"quantity\":999}]}" \
    -o /dev/null &
done

wait

echo "Kraj: $(date '+%H:%M:%S')"
echo "Poslato 30 ordera, cekam saga da se zavrsi..."
sleep 8

# Statistika iz baze
echo ""
echo "=== Stanje u bazi ==="
podman compose exec postgres psql -U app -d ecommerce -t -c "
SELECT status, COALESCE(rejection_reason, '-') AS reason, COUNT(*) AS broj
FROM orders
WHERE customer_id LIKE 'BULK-%'
GROUP BY status, rejection_reason
ORDER BY status, rejection_reason;
"

# Particije po instancama
echo ""
echo "=== Particije po Inventory instanci ==="
for c in $(podman compose ps -q inventory-service); do
  short=$(podman ps --filter id=$c --format '{{.Names}}')
  count=$(podman logs $c 2>&1 | grep "partitions assigned: \[orders.created" | tail -1 | grep -o 'orders.created-[0-9]*' | wc -l)
  echo "  $short -> $count particija"
done

echo ""
echo "Pogledaj sad Grafanu/Prometheus za grafove"