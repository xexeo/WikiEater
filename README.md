# WikiEater

Crawler educado para criar backup local de wikis de jogos, com foco em texto útil para análise qualitativa (Grounded Theory/codificação de sentenças) e preservação de tags/classes de classificação de páginas.

## O que este programa faz

- Mantém uma base SQLite com:
  - jogos ativos e wiki principal por jogo;
  - links descobertos;
  - links já baixados/falhos;
  - caminho do arquivo local salvo para cada link;
  - log de execução.
- Retoma do ponto onde parou (incluindo encerramento por sinal).
- Nunca duplica link por jogo (`UNIQUE(game_id, url)`).
- Salva HTML limpo (sem propaganda/imagens/scripts), preservando texto e classes/tags relevantes.
- Usa crawler com múltiplas threads configuráveis (padrão: 2), distribuindo trabalho em round-robin entre jogos para evitar carga concentrada em uma única wiki.

## Lista inicial filtrada

Foram removidos os jogos sem inventário e mantidos apenas os de perspectiva primeira pessoa, terceira pessoa ou grupo.

A lista inicial está em `wikieater/config.py` (`FILTERED_GAMES`) e é inserida na base automaticamente.

## Uso

```bash
python -m wikieater.cli --db ./wikieater.sqlite3 --wiki-dir ./wikis run --threads 2
python -m wikieater.cli --db ./wikieater.sqlite3 status
python -m wikieater.cli --db ./wikieater.sqlite3 logs --limit 100
```

## Interface de controle

A interface é por CLI:

- `status`: jogos sendo baixados, wiki por jogo, links identificados, links baixados, taxa de conclusão.
- `logs`: exibe histórico do processo.
- `run`: executa o crawler e respeita SIGINT/SIGTERM com persistência de estado.

## Compatibilidade com análise Grounded Theory

A saída preserva:

- texto principal útil para codificação aberta/axial;
- classes e tags (`class`, `id`, `rel`, alguns `data-*`) para apoiar agregações por tipo de conteúdo;
- categorias detectadas em links de categoria, armazenadas no HTML limpo como `page-category`.

## Pontos a confirmar (perguntas)

1. A curadoria da lista filtrada de jogos está correta para seu critério de "inventário"?
2. Você quer limitar o crawler só a páginas de itens (ex.: namespaces/categorias específicas), ou manter todo conteúdo interno da wiki e filtrar na etapa analítica?
3. Você deseja também persistir classes/tags em colunas estruturadas no SQLite (além de embutir no HTML limpo)?
4. Qual limite de páginas por jogo (se houver) para a primeira execução de backup?
