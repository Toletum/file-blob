# file-blob

Plugin para CDAP (Spark sink `FileBlob`) que escribe el campo `body` de los registros de entrada en una ruta de destino.

## Visibilidad y rama
- Público.
- Rama por defecto: no verificado (no se detectó `.git` accesible).

## Stack
- Java 8 (`maven-compiler-plugin` con source/target 1.8).
- Maven.
- CDAP 6.5.1 (`cdap-api`, `cdap-etl-api`, `cdap-etl-api-spark`, `cdap-formats`, `hydrator-test`) con Spark 2.1.3 (Scala 2.11).
- Hadoop `FileSystem` (`org.apache.hadoop`) para la escritura.
- `cdap-maven-plugin` 1.1.0 para generar el JSON del artefacto en `prepare-package`.

## Estructura
- `pom.xml`: dependencias, plugins y propiedades de versión.
- `src/main/java/org/toletum/cdap/plugin/FileBlob.java`: plugin principal (`prepareRun` y `run`, y clase `FileBlobConfig`).
- `widgets/FileBlob-sparksink.json`: definición del widget para la UI de CDAP.
- `docs/FileBlob-sparksink.md`: descripción del plugin (todavía copiada de un ejemplo).
- `README.md`: pasos de build e instalación.

## Comandos
Desde el README:
- Build con tests: `mvn clean package`
- Build sin tests: `mvn clean package -DskipTests`
- Cargar en CDAP (dentro del contenedor `cdap-sandbox`, `docker exec -ti cdap-sandbox bash`):
  `cdap cli load artifact target/file-blob-0.0.1-SNAPSHOT.jar config-file target/file-blob-0.0.1-SNAPSHOT.json`

## Configuración
- Propiedad del plugin `destinationPath` (acepta macros `${...}`): ruta de destino del fichero.
- Sin variables de entorno.

## Convenciones para agentes
- No cambiar versiones de CDAP/Spark sin comprobar la compatibilidad con el sandbox.
- Las dependencias de CDAP/Spark son `provided`; no empaquetarlas.
- Cualquier cambio de propiedades del plugin debe reflejarse en `widgets/FileBlob-sparksink.json` y en la clase `FileBlobConfig`.

## Estado
- No hay tests en `src/test` (la configuración de surefire existe, pero no hay clases de test).
- `FileBlob.run` hace `javaRDD.collect()` y carga todos los registros en el driver: riesgo de memoria con datos grandes.
- `run` escribe `record.get("body")` sin validar que el campo exista ni su tipo (se espera `byte[]`).
- `@Description` de la clase y `docs/FileBlob-sparksink.md` son textos de ejemplo ("push to log a Sample") que no describen el comportamiento real.
- Hay código de depuración en `prepareRun` (`LOG.info("TEST"...)`, `classBuildTimeMillis`) que conviene limpiar.
- `pom.xml` usa un repositorio de plugins de Sonatype snapshots (`oss.sonatype.org`), que está obsoleto.
- Estado de uso real: no verificado.
