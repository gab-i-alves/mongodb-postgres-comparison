O projeto é composto por quatro scripts principais:

1. **pre-processing-script.py**: Responsável pelo pré-processamento dos dados de tweets, incluindo limpeza de texto, remoção de URLs, menções e hashtags, e análise de sentimento usando as bibliotecas TextBlob e VADER.

2. **postgres-script.py**: Responsável pela importação dos dados de tweets e análise de sentimento para um banco de dados PostgreSQL. Ele cria um novo banco de dados, tabelas otimizadas com índices e realiza a importação dos dados em lotes.

3. **mongodb-script.py**: Responsável pela importação dos dados de tweets e análise de sentimento para um banco de dados MongoDB. Ele cria um novo banco de dados, uma coleção para armazenar os dados e realiza a importação em lotes.

4. **comparisons.py**: Responsável por executar um benchmark comparativo entre o PostgreSQL e o MongoDB, testando diferentes tipos de consultas, como consultas simples (CRUD), consultas de busca de texto, operações de agregação e consultas com joins. O script também gera visualizações comparando o desempenho dos bancos de dados.

---

1. **pre-processing-script.py**:
   - Este script realiza a limpeza e pré-processamento dos dados de tweets, incluindo a remoção de URLs, menções e hashtags, além da análise de sentimento usando as bibliotecas TextBlob e VADER.
   - O script trata de forma robusta os erros que podem ocorrer durante o pré-processamento e a análise de sentimento, registrando-os em um arquivo de log.
   - Não há diferenças significativas entre o PostgreSQL e o MongoDB nesta etapa, pois o script está focado em operações de pré-processamento e análise de dados, independentemente do banco de dados utilizado.

2. **postgres-script.py**:
   - Este script é responsável pela importação dos dados de tweets e análise de sentimento para o banco de dados PostgreSQL.
   - Ele cria um novo banco de dados, tabelas otimizadas com índices, e realiza a importação dos dados em lotes utilizando o `execute_values` do Psycopg2 para melhorar o desempenho.
   - Destacam-se as seguintes diferenças em relação ao MongoDB:
     - O PostgreSQL utiliza uma abordagem relacional, com tabelas separadas para usuários, tweets e análise de sentimento, com chaves estrangeiras entre elas.
     - A consulta de dados envolve junções (joins) entre as tabelas para obter informações completas sobre os tweets.
     - O PostgreSQL possui recursos avançados de consulta de texto, como o uso de `to_tsvector` e `to_tsquery` para busca de texto.

3. **mongodb-script.py**:
   - Este script é responsável pela importação dos dados de tweets e análise de sentimento para o banco de dados MongoDB.
   - Ele cria um novo banco de dados e uma coleção para armazenar os dados, utilizando uma estrutura de documento para representar todas as informações de um tweet.
   - As principais diferenças em relação ao PostgreSQL são:
     - O MongoDB possui uma abordagem orientada a documentos, onde todas as informações de um tweet (incluindo usuário, texto, análise de sentimento) são armazenadas em um único documento.
     - Não há a necessidade de realizar joins entre tabelas, pois as informações estão denormalizadas dentro do documento.
     - O MongoDB possui recursos nativos de busca de texto, utilizando o operador `$text` para consultas de texto.
     - A importação dos dados é realizada em lotes, utilizando a API `bulk_write` do MongoDB para melhorar o desempenho.

4. **comparisons.py**:
   - Este script realiza um benchmark comparativo entre o PostgreSQL e o MongoDB, testando diferentes tipos de consultas:
     - Consultas simples (CRUD)
     - Consultas de busca de texto
     - Operações de agregação
     - Consultas com joins
   - O script executa cada consulta múltiplas vezes e calcula métricas como tempo médio, mediano, desvio padrão, mínimo e máximo.
   - As diferenças entre os bancos de dados ficam evidentes nesta etapa:
     - Para consultas simples e busca de texto, o MongoDB tende a ter um desempenho melhor do que o PostgreSQL.
     - Para operações de agregação, o PostgreSQL apresenta um melhor desempenho devido à sua capacidade de realizar cálculos complexos diretamente no banco de dados.
     - Nas consultas com joins, o PostgreSQL também demonstra um melhor desempenho, pois sua abordagem relacional é mais eficiente para este tipo de operação.
   - O script também gera visualizações comparando o desempenho dos bancos de dados em diferentes categorias de consultas.

Em resumo, o MongoDB se destaca no processamento de dados semi-estruturados, como tweets, com sua abordagem orientada a documentos. Ele apresenta melhor desempenho em consultas simples e de texto. Já o PostgreSQL tem vantagem em operações de agregação e joins, devido à sua natureza relacional e recursos avançados de consulta de texto. A escolha entre os dois bancos de dados dependerá do tipo de carga de trabalho e dos requisitos específicos da aplicação.

---

1. **Consultas Simples (CRUD)**:
   - PostgreSQL:
     - A consulta de exemplo é:
       ```sql
       SELECT * FROM tweets t
       JOIN sentiment_analysis s ON t.tweet_id = s.tweet_id
       WHERE s.target = 4
       LIMIT 100
       ```
     - Essa consulta realiza um join entre as tabelas de tweets e análise de sentimento para obter todas as informações relevantes dos tweets com target igual a 4, retornando os primeiros 100 resultados.
   - MongoDB:
     - A consulta de exemplo é:
       ```javascript
       db.tweets.find({
           "sentiment_analysis.target": 4
       }).limit(100)
       ```
     - Essa consulta busca diretamente os documentos na coleção de tweets que tenham um valor de 4 no campo `sentiment_analysis.target`, retornando os primeiros 100 resultados.

2. **Consultas de Busca de Texto**:
   - PostgreSQL:
     - A consulta de exemplo é:
       ```sql
       SELECT * FROM tweets
       WHERE to_tsvector('english', cleaned_text) @@ to_tsquery('english', 'love')
       LIMIT 100
       ```
     - Essa consulta utiliza a função `to_tsvector` para criar um índice de texto baseado no campo `cleaned_text`, e a função `to_tsquery` para realizar a busca pela palavra "love". O operador `@@` é usado para realizar a correspondência entre o texto indexado e a consulta de texto.
   - MongoDB:
     - A consulta de exemplo é:
       ```javascript
       db.tweets.find({
           "$text": {
               "$search": "love"
           }
       }).limit(100)
       ```
     - O MongoDB possui um índice de texto nativo, e a consulta utiliza o operador `$text` para realizar a busca pela palavra "love" em todos os documentos da coleção.

3. **Operações de Agregação**:
   - PostgreSQL:
     - A consulta de exemplo é:
       ```sql
       SELECT 
           target,
           COUNT(*),
           AVG(vader_compound)
       FROM sentiment_analysis
       GROUP BY target
       ```
     - Essa consulta utiliza a cláusula `GROUP BY` para agrupar os resultados por `target`, e então calcula a contagem de cada grupo e a média do campo `vader_compound`.
   - MongoDB:
     - A consulta de exemplo é:
       ```javascript
       db.tweets.aggregate([
           {
               "$group": {
                   "_id": "$sentiment_analysis.target",
                   "count": { "$sum": 1 },
                   "avg_vader_compound": { "$avg": "$sentiment_analysis.vader_compound" }
               }
           }
       ])
       ```
     - O MongoDB utiliza o framework de agregação, que permite realizar cálculos complexos diretamente no banco de dados. Essa consulta agrupa os documentos por `sentiment_analysis.target`, contando o número de documentos em cada grupo e calculando a média do campo `vader_compound`.

4. **Consultas com Joins**:
   - PostgreSQL:
     - A consulta de exemplo é:
       ```sql
       SELECT t.*, u.*, s.*
       FROM tweets t
       JOIN users u ON t.user_id = u.user_id
       JOIN sentiment_analysis s ON t.tweet_id = s.tweet_id
       LIMIT 100
       ```
     - Essa consulta realiza joins entre as tabelas de tweets, usuários e análise de sentimento para obter todas as informações relevantes dos tweets, incluindo os detalhes do usuário e a análise de sentimento.
   - MongoDB:
     - A consulta de exemplo é:
       ```javascript
       db.tweets.aggregate([
           {
               "$lookup": {
                   "from": "users",
                   "localField": "user.username",
                   "foreignField": "username",
                   "as": "user_details"
               }
           },
           { "$limit": 100 }
       ])
       ```
     - O MongoDB utiliza o operador `$lookup` para realizar um join entre a coleção de tweets e a coleção de usuários, com base no campo `user.username`. Essa abordagem denormalizada permite obter as informações do usuário diretamente no documento do tweet.
