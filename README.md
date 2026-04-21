# WikiEater

Aplicação local em **Python + Tkinter** para backup de wikis de jogos com:

- crawler com alternância entre jogos (round-robin);
- controle de threads e taxa de rede em tempo real;
- verificação de `robots.txt` antes de cada fetch (com opção de bypass);
- persistência de estado em SQLite para retomar de onde parou;
- extração e salvamento de HTML limpo por jogo.

## Arquivos principais

- `crawler_tk.py`: aplicação completa (UI + crawler + persistência).
- `crawler_config.json`: configuração runtime e catálogo de jogos.
- `crawler_state.sqlite3`: estado persistido da execução.

## Executar

```bash
python crawler_tk.py
```

## Verificação rápida

```bash
python -m py_compile crawler_tk.py
```

## Observações

- O programa cria diretórios de saída automaticamente em `wikis/`.
- Estratégia segura: sem browser/headless; somente HTTP com biblioteca padrão.
