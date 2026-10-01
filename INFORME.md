# Informe - TP Coordinación

Este informe explica cómo se coordinan las instancias de Sum y Aggregation, y cómo escala el sistema respecto a la cantidad de clientes, el volumen de datos y la cantidad de controles.

## 1. Visión general

Las réplicas de Sum compiten por una misma cola (`INPUT_QUEUE`), por lo que RabbitMQ reparte los registros entre ellas.

Cada Aggregation escucha su propia clave (`{AGGREGATION_PREFIX}_{ID}`) de un exchange direct.

El Join espera un top parcial de cada Aggregation y arma el top final.

Ningún control tiene nombres escritos a mano, colas, prefijos, cantidades de réplicas e ID salen de variables de entorno. Por eso el mismo código sirve para todos los escenarios, incluido el de nombres al azar.

## 2. Aislamiento entre clientes

El `MessageHandler` del Gateway genera un `client_id` (`uuid4`) por cliente y lo agrega a todos los mensajes. Sum, Aggregation y Join mantienen un estado independiente por `client_id`, de modo que varios clientes pueden ser atendidos a la vez sin mezclarse. Al volver, el Gateway entrega cada resultado al cliente cuyo `client_id` coincide.

## 3. Coordinación entre las réplicas de Sum

Un problema que nos encontramos a la hora de agregar mas Sums es que los registros de un cliente quedan repartidos entre las S réplicas, pero el mensaje de fin (EOF) es uno solo y lo recibe una única réplica. Las demás no se enteran de que el cliente terminó, y además pueden tener registros del cliente todavía en vuelo.

Esto lo solucionamos con cuatro tipos de mensaje:

| Mensaje | Emisor → Receptor | Contenido |
|---|---|---|
| `EOF` | Gateway → Sum (una réplica) | `client_id`, `total_records`: cantidad de registros que envió el cliente |
| `FLUSH` | Sum → todos los Sum (exchange de control) | `client_id` |
| `COUNT` | Sum → todos los Aggregation | `client_id`, cantidad de registros originales que cubre lo enviado |
| `TOTAL` | Sum → todos los Aggregation | `client_id`, `total_records` |

Pasos:

1. La réplica que recibe el `EOF` envía `TOTAL` a los Aggregation y publica `FLUSH` a todas las réplicas de Sum (incluida ella misma).
2. Cada Sum, al recibir `FLUSH`, marca al cliente como cerrado, envía sus totales parciales por fruta (`DATA`) y un `COUNT` con los registros que esos totales cubren.
3. Cada Aggregation da por completo a un cliente cuando la suma de los `COUNT` recibidos es igual al `TOTAL` (por lo que si llegase antes el `EOF` que uno de los registros, con un flush inmediato, también funcionaría).

### Concurrencia dentro del Sum: 
Cada réplica tiene dos hilos, el principal consume registros y el de control consume los `FLUSH`. El estado compartido (`state_by_client`, `closed_clients`) se protege con un lock. Como las conexiones de pika no son thread-safe, cada hilo usa sus propias conexiones.

## 4. Coordinación entre las réplicas de Aggregation

Para no procesar lo mismo en todas las réplicas, los datos se reparten por fruta. Cada fruta tiene un único Aggregation dueño, `crc32(fruta) % A`. Se usa `crc32` y no el `hash()` de Python porque este último usa una semilla aleatoria distinta por proceso, y las réplicas de Sum no coincidirían en el dueño de cada fruta. No hace falta un hash criptográfico, solo uno determinista y rápido.

- `DATA` va solo al dueño de la fruta.
- `COUNT` y `TOTAL` van a todos los Aggregation. Son mensajes livianos y cada uno los necesita para saber cuándo cerró el cliente.
- Cuando un Aggregation completa un cliente, envía su top parcial al Join, incluso si está vacío (no le tocó ninguna fruta).
- El Join cuenta un top por cada Aggregation (`AGGREGATION_AMOUNT`). Como las frutas son disjuntas entre Aggregations, el top final se arma sin duplicados y sin tener que volver a sumar.

## 5. Escalabilidad

**Clientes:** El estado se separa por `client_id` en todos los controles, y los clientes se atienden en paralelo por las mismas colas. Los mensajes de control de un cliente son independientes de los de los demás.

**Volumen de datos:** El protocolo de control no crece con la cantidad de registros, el `EOF` lleva un contador y no hay un mensaje por registro. Cada Sum guarda por cliente un acumulado por fruta (memoria proporcional a las frutas distintas, no a los registros) y le envía a los Aggregation un `DATA` por fruta distinta. El volumen entre Sum y Aggregation queda acotado por la cantidad de frutas distintas, sin importar cuántos registros se hayan procesado.

**Cantidad de controles:**

- Sum: se agregan réplicas sin cambiar el código. RabbitMQ reparte los registros entre ellas.
- Aggregation: se agregan réplicas y el trabajo se reparte por fruta, sin datos ni cómputo redundante.
- Costo del control: por cliente se envían `S` mensajes `FLUSH`, `A` mensajes `TOTAL` y `S×A` mensajes `COUNT`. Crece con las réplicas, pero no con los datos, y son mensajes muy chicos.

## 6. Cierre ordenado (graceful shutdown)

Sum, Aggregation y Join manejan `SIGTERM` y `SIGINT`. El handler no toca la conexión directamente (corre en el mismo hilo que está bloqueado consumiendo), lanza un hilo auxiliar que detiene el consumo. El mensaje en curso termina y se confirma (ack) antes de salir. Luego se hace `join` del hilo de control del Sum y se cierran todas las conexiones. La lógica común está en `common/shutdown.py`.

## 7. Limitaciones conocidas

- **Reparto por fruta:** puede quedar desparejo. Con pocas frutas distintas, algún Aggregation puede quedar con poco trabajo o ninguno. El sistema no puede aprovechar más Aggregations que frutas distintas.
- **`closed_clients`:** en cada Sum crece con cada cliente atendido y no se limpia. Con muchísimos clientes consumiría memoria.
- **Mensajes de control `COUNT`:** crecen como `S×A` por cliente.

## 8. Lo más desafiante del trabajo
 
Lo más desafiante fue el escenario 3, la coordinación entre las réplicas de Sum. Con una sola réplica, el `EOF` alcanza para saber que un cliente terminó. Con varias, deja de alcanzar:
 
- Los registros de un cliente quedan repartidos entre las réplicas, pero el `EOF` lo recibe una sola. Las demás no saben que el cliente terminó.
- Hubo que decidir cuándo cada réplica envía lo acumulado y cuándo un Aggregation puede dar por completo a un cliente. La solución fue el conteo de registros (`TOTAL` contra la suma de `COUNT`).
- Un registro puede llegar a una réplica después de que ya hizo el flush. Por eso se marca al cliente como cerrado y se reenvían de inmediato los registros tardíos.
- Para que el conteo sea confiable hay que garantizar el orden, el `DATA` y el `COUNT` de un Sum salen por la misma conexión.
- Cada réplica pasó a tener dos hilos (registros y control) compartiendo estado, lo que obligó a proteger las secciones críticas con un `Lock` y a darle a cada hilo sus propias conexiones.
Los escenarios 4 y 5 se apoyan en este protocolo, una vez resuelto, el reparto por fruta entre Aggregations fue un cambio simple.