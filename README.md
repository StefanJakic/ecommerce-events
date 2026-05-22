# ecommerce-events

Mali event-driven sistem za e-commerce domen. Tri Spring Boot servisa
povezana preko Apache Kafke, plus deljeni modul sa event ugovorima.

Note:
Imajte u vidu da kda prvi put pokrecete compose up, moze da traje ~20min

## Moduli

| Modul | Uloga |
|---|---|
| `common` | Deljeni event ugovor (`OrderCreatedEvent`) |
| `order-api` | REST servis: kreira porudzbinu + upisuje outbox red (atomicno) |
| `outbox-relay` | Cita outbox tabelu i objavljuje evente na Kafku |
| `inventory-service` | Kafka konzument: rezervise zalihe |

## Arhitektura

```
Klijent --HTTP--> Order API --(ista TX)--> orders + outbox tabela
                       ^   ^                      |
                       |   |     Outbox Relay <---+  (FOR UPDATE SKIP LOCKED)
                       |   |           |
                       |   |           +--> Kafka topic: orders.created
                       |   |                       |
                       |   |     Inventory Service <-  (@RetryableTopic)
                       |   |           |
                       |   |  uspeh ---+--(ista TX)-> zalihe + inventory_outbox
                       |   |  odbijeno-+  (samo outbox red sa razlogom)
                       |   |                              |
                       |   |     Inventory Relay  <-------+
                       |   |              |
                       |   +-- inventory.reserved  -> status CONFIRMED
                       +------ inventory.rejected  -> status REJECTED
```

Choreography saga sa POZITIVNOM i NEGATIVNOM granom:
- `CREATED` -> tek upisan
- `CONFIRMED` -> Inventory potvrdio rezervaciju
- `REJECTED` -> Inventory odbio (nepoznat SKU ili nedovoljno zaliha)

DLT (orders.created-dlt) sad sluzi samo za TEHNICKE greske
(malformiran JSON, bug, baza). Poslovne greske idu kao InventoryRejected
event, da bi klijent mogao da sazna sta se desilo sa porudzbinom.

## Kljucne dizajn odluke

- **Transactional outbox** — Order API upisuje order i outbox red u istoj
  DB transakciji. Nema ordera bez eventa ni obrnuto.
- **SKIP LOCKED** — Outbox Relay sme da se skalira na vise instanci bez
  duplikata. Sa jednom instancom radi identicno.
- **Idempotencija** — Inventory proverava `eventId` u `processed_event`
  tabeli pre obrade. Pokriva at-least-once isporuku (relay/Kafka retry).
- **Non-blocking retry** — `@RetryableTopic` premesta neuspele poruke na
  zasebne retry topике; glavni `orders.created` se nikad ne blokira.
- **Optimistic locking** — `@Version` na `InventoryItem`; konkurentni
  konzumenti se ne otimaju oko reda, sudar se retry-uje.

## Testovi

Testovi pokrivaju sva tri modula. Ima ih dve vrste:
- **Unit testovi** (Mockito, bez Springa) - brzi, hvataju logicke greske
- **Integracioni testovi** (`*IT`, Testcontainers Postgres + Kafka) - sporiji, hvataju greske integracija

### Inventory Service

| Test | Tip | Sta dokazuje |
|---|---|---|
| `InventoryServiceTest` | Unit | `reserve()` logika - happy, idempotency, oba rejection razloga |
| `InventoryConsumerIT` | IT | Kafka -> baza -> zalihe; isti event dvaput skida zalihe SAMO jednom |
| `InventoryReservedPublishingIT` | IT | Uspeh -> InventoryReserved event na `inventory.reserved` |
| `InventoryRejectedPublishingIT` | IT | Rejection -> InventoryRejected event na `inventory.rejected` |

### Order API

| Test | Tip | Sta dokazuje |
|---|---|---|
| `OrderServiceTest` | Unit | `createOrder()` upisuje I order I outbox red u istoj sesiji; JSON payload sadrzi sve podatke |
| `OrderControllerTest` | Slice (`@WebMvcTest`) | HTTP validacija: prazan customerId, prazna items lista, quantity 0 -> 400 |
| `OrderApiEndToEndIT` | IT | POST /orders -> outbox upisan -> simulirani InventoryReserved/Rejected -> status CONFIRMED/REJECTED |

### Outbox Relay

| Test | Tip | Sta dokazuje |
|---|---|---|
| `OutboxRelayTest` | Unit | Salje na `orders.created`; redosled send -> flush -> markPublished; prazan batch ne radi nista |

### Pokretanje

```bash
# samo brzi unit i slice testovi (ne treba Docker)
mvn test -Dtest='*Test'

# svi testovi (potreban Docker za Testcontainers)
mvn test

# samo jedan modul
mvn -pl inventory-service test
mvn -pl order-api test
mvn -pl outbox-relay test
```

Integracioni testovi (`*IT`) podizu pravu Postgres i pravu Kafku u Docker
kontejnerima preko Testcontainers, pa **Docker mora biti pokrenut** dok ih
pokreces. Svaki test ima svoj `group-id` (sa timestamp suffix-om) tako da
ne dele consumer offset izmedju runova.

Iz IntelliJ-a: desni klik na test klasu/metodu -> Run.

## Observability

Sistem koristi **push-based** observability sa OpenTelemetry Collector kao centralnim hub-om:

```
   Servisi (OTLP push)  →  OTel Collector  →  Prometheus (scrape)
                                            →  Tempo (traces)
```

Prednost ovog pristupa:
- **Servisi su non-web** gde im to nije primarna funkcija (Inventory, Outbox Relay)
- **Skaliranje radi out-of-the-box**: `podman compose up -d --scale inventory-service=3` ne razbija ni jednu konfiguraciju
- **Prometheus ne mora da zna gde su servisi** - scrape-uje samo Collector
- Industrijski standard - isto se koristi u K8s/AWS produkciji

Servisi:
- **OTel Collector** (port 4318 OTLP HTTP, 8889 Prometheus exporter)
- **Prometheus** (port 9090) - scrape-uje samo Collector
- **Tempo** (port 3200) - storage za tragove
- **Grafana** (port 3000, anonymous login) - dashboard

### Otvori u browser-u

- `http://localhost:3000` - Grafana, dashboard "Ecommerce Events Overview"
- `http://localhost:9090` - Prometheus (PromQL upiti)
- `http://localhost:9090/targets` - vidi da Collector je scrape-ovan

### Traceability ("Sta se desilo sa orderom 123?")

Svaki log ima 6 polja koja olaksavaju pretragu:

| Polje | Sta govori |
|---|---|
| `traceId` | distributed trace - povezuje sve sa istog HTTP zahteva ili Kafka poruke |
| `service` | koji servis je logovao (order-api, outbox-relay, inventory-service) |
| `orderId` | poslovni identifikator porudzbine |
| `eventId` | konkretan event - za debug duplikata |
| `eventType` | tip event-a (OrderCreated, InventoryReserved, InventoryRejected) |
| `status` | stanje order-a (CREATED, CONFIRMED, REJECTED) |

Logovi su u JSON formatu - lako pretrazivi u Loki/ELK:

```json
{"time":"2026-05-21T13:45:23.123+02:00","service":"inventory-service","traceId":"a1b2c3","orderId":"ORDER-123","eventId":"evt-1","eventType":"OrderCreated","status":"CONFIRMED","level":"INFO","msg":"Skinuto 2 x SKU-001 (ostalo 98)"}
```

Pretraga `orderId="ORDER-123"` kroz Loki/jq daje ti potpunu sekvencu kroz sva tri servisa.

### Sta metrike pokazuju

| Metrika | Sta govori |
|---|---|
| `inventory_reserved_count_total` | broj uspesnih rezervacija |
| `inventory_rejected_count_total{reason=...}` | broj odbijenih po razlogu (UNKNOWN_SKU, INSUFFICIENT_STOCK, SYSTEM_ERROR) |
| `outbox_pending_size` | broj neobjavljenih outbox redova - INDIKATOR LAG-a |
| `http_server_requests_seconds_bucket` | latencija HTTP zahteva (p95, p99) |

### Skaliranje

Inventory Service i Outbox Relay nemaju `ports:` ni `container_name:` - skaliranje je trivijalno:

```bash
podman compose up -d --scale inventory-service=3 --scale outbox-relay=2
```

Tri instance Inventory dele Kafka particije (12 particija / 3 instance = 4 po instanci).
Sve instance i dalje saliju metrike u isti OTel Collector koji ih agregira po `service.name` tag-u.

## DLT i SYSTEM_ERROR

Sistem ima tri vrste finalnih statusa za order:
- `CONFIRMED` - sve proslo OK
- `REJECTED` sa razlogom `UNKNOWN_SKU` ili `INSUFFICIENT_STOCK` - poslovna greska
- `REJECTED` sa razlogom `SYSTEM_ERROR` - tehnicka greska (iz DLT)

DLT vise nije "tihi" - kad poruka zavrsi na DLT (posle 3 retry-ja tehnickih
gresaka), `@DltHandler` objavi `InventoryRejected` sa `SYSTEM_ERROR` razlogom.
Order API to konzumira i prebaci order u `REJECTED`. Klijent uvek sazna ishod.

## Pokretanje

Potreban Docker. Iz root foldera:

```bash
docker compose up --build
```

Podize Kafku, Postgres i sva tri servisa. Order API slusa na portu 8080.

### Kreiranje porudzbine

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
        "customerId": "CUST-001",
        "items": [
          { "sku": "SKU-001", "quantity": 2 },
          { "sku": "SKU-002", "quantity": 1 }
        ]
      }'
```

U logovima `inventory-service` videces "Skinuto ... " i "uspesno rezervisan".
U `order-api` videces zatim "Order ... prebacen u status CONFIRMED" (saga zatvorena).

Provera statusa u bazi:

```bash
podman compose exec postgres psql -U app -d ecommerce \
  -c "SELECT id, status FROM orders ORDER BY created_at DESC LIMIT 3;"
```

## Demo: skaliranje konzumenta

Pocetni topic `orders.created` ima 12 particija (videti `docker-compose.yml`),
pa Inventory moze da se skalira do 12 korisnih instanci.

```bash
# 3 instance Inventory servisa
docker compose up --build --scale inventory-service=3

# u drugom terminalu - skaliraj naviše dok sistem radi
docker compose up -d --scale inventory-service=6

# gledaj rebalansiranje particija u logovima
docker compose logs -f inventory-service
```

Outbox Relay se skalira na isti nacin (`--scale outbox-relay=2`) -
`SKIP LOCKED` garantuje da nema duplih objava.

## Demo: REJECTED status (poslovne greske)

Posaljes porudzbinu sa nepoznatim SKU. Order API vrati 201 (jer on samo prima
zahtev), pa za sekundu order predje u status REJECTED.

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{ "customerId": "CUST-009",
        "items": [ { "sku": "SKU-NE-POSTOJI", "quantity": 1 } ] }'
```

U Inventory logu: `Order ... odbijen: nepoznat SKU SKU-NE-POSTOJI`
U Order API logu: `Order ... prebacen u status REJECTED, razlog: UNKNOWN_SKU`

Provera u bazi:

```bash
podman compose exec postgres psql -U app -d ecommerce \
  -c "SELECT id, status, rejection_reason FROM orders ORDER BY created_at DESC LIMIT 5;"
```

**Nedovoljno zaliha** - SKU-042 ima 10 komada, trazi 999:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{ "customerId": "CUST-010",
        "items": [ { "sku": "SKU-042", "quantity": 999 } ] }'
```

Order ce dobiti status REJECTED, razlog INSUFFICIENT_STOCK.

## Demo: DLT (tehnicke greske)

DLT (`orders.created-dlt`) sad sluzi samo za tehnicke greske - bug, baza
nedostupna, malformiran JSON. Poslovne greske idu kroz REJECTED flow iznad.

Provera DLT topica:

```bash
podman compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders.created-dlt \
  --from-beginning
```

## Pocetne zalihe

| SKU | Kolicina |
|---|---|
| SKU-001 | 100 |
| SKU-002 | 50 |
| SKU-042 | 10 |

Definisano u `inventory-service/src/main/resources/schema.sql`.

## Otvaranje u IntelliJ-u

`File > Open` -> izaberi root `pom.xml` -> `Open as Project`.
IntelliJ prepozna sva 4 modula. Build radi sa ugradjenim (bundled) Mavenom,
nije potreban zaseban Maven na sistemu.

## Napomene za produkciju (sledeci koraci)

- JSON serijalizacija je ovde radi jednostavnosti; za vise potrosaca
  i striktnu evoluciju seme -> Avro + Schema Registry.
- Outbox poll petlja -> moze se zameniti Debezium CDC-jem (bez polling
  kasnjenja, cita transaction log baze).
- Logovi idu u konzolu; za produkciju -> strukturirani JSON + ELK/Loki.
- Auto-skaliranje -> KEDA na consumer lag, max replika = broj particija.

