# WikiEater

Aplicação local em **Python + Tkinter** para backup de wikis de jogos com:

- crawler com alternância entre jogos (round-robin);
- controle de threads e taxa de rede em tempo real;
- verificação de `robots.txt` antes de cada fetch (com opção de bypass);
- fallback opcional para renderização de conteúdo via JavaScript;
- persistência de estado em SQLite para retomar de onde parou;
- extração e salvamento de HTML limpo por jogo.

## Capacidades

O programa atualmente oferece:

- Interface desktop em Tkinter para operar o crawler localmente.
- Controle manual de `threads`, `requests per minute`, `bypass_robots` e meta de cobertura.
- Controle manual de `max_failures` para decidir quantas falhas temporárias uma página pode acumular antes de parar em `failed`.
- Opção `Renderizar JS` para tentar recuperar conteúdo montado no cliente quando o HTML inicial vier incompleto.
- Botões de `Iniciar`, `Pausar`, `Retomar` e `Parar` para controlar a execução.
- Inicialização do crawl apenas ao clicar em `Iniciar`.
- Verificação automática ao iniciar: se a base estiver vazia ou ausente, ela é criada a partir de `crawler_config.json`.
- Botão `Criar base da config` para popular a base manualmente a partir do JSON de configuração.
- Botão `Resetar base` para apagar o banco SQLite e todos os arquivos gerados em `wikis/`, recriando tudo do zero.
- Persistência de estado em `crawler_state.sqlite3`, permitindo continuar execuções interrompidas.
- Banco SQLite com catálogo de jogos, URLs descobertas, status de fetch, links entre páginas, tags extraídas e estado geral do crawler.
- Separação entre páginas `blocked` por robots e páginas `failed` por erro técnico.
- Round-robin entre jogos para evitar concentrar todas as requisições em uma única wiki.
- Rate limiter global em requisições por minuto, ajustável em runtime.
- Workers paralelos configuráveis para crawling concorrente.
- Pausa cooperativa dos workers sem perder o progresso persistido.
- Reconfiguração em runtime sem precisar reiniciar a aplicação.
- Normalização de URLs para reduzir duplicatas.
- Deduplicação por URL canônica dentro de cada wiki.
- Catálogo de jogos e seeds carregados de `crawler_config.json`.
- Descoberta automática de links internos a partir das páginas baixadas.
- Restrição de descoberta para links do mesmo host da wiki atual.
- Filtro conservador para evitar áreas como login, perfil, edição, histórico e páginas administrativas.
- Consulta de `robots.txt` por host com cache local para evitar leituras repetidas.
- Modo `bypass_robots` que, quando ativado, ignora a consulta de `robots.txt`.
- Download via HTTP usando apenas biblioteca padrão do Python.
- Fallback opcional com Playwright para páginas cujo conteúdo importante depende de JavaScript.
- Extração preferencial apenas da região principal da página também no fallback JS.
- Estratégia padrão sem browser/headless, com fallback opcional.
- Tratamento de timeout, erro HTTP e erro de rede.
- Reenvio automático de URLs com falha enquanto houver tentativas restantes.
- Reativação automática de páginas `failed` quando `max_failures` é aumentado acima do total de falhas já registrado.
- Extração de categorias/tags a partir do HTML das páginas.
- Limpeza do HTML para remover scripts, estilos, mídias e blocos periféricos antes de salvar.
- Priorização de regiões principais de conteúdo, quando detectáveis.
- Salvamento do HTML limpo por jogo em `wikis/<slug-do-jogo>/`.
- Criação automática dos diretórios de saída.
- Tabela de progresso por jogo com contadores de descobertas, fila, fetch em andamento, sucesso, falha e porcentagem concluída.
- Log operacional em tempo real dentro da interface.
- Fechamento gracioso da aplicação com parada dos workers, salvamento de configuração e registro de estado.

## Arquivos principais

- `crawler_tk.py`: aplicação completa (UI + crawler + persistência).
- `crawler_config.json`: configuração runtime e catálogo de jogos.
- `crawler_state.sqlite3`: estado persistido da execução.

## Executar

```bash
python crawler_tk.py
```

## Instalar Playwright no Windows

Se você quiser usar a opção `Renderizar JS`, instale o Playwright e os navegadores dele:

```powershell
python -m pip install --upgrade pip
python -m pip install playwright
python -m playwright install chromium
```

Se o comando `python` não estiver no PATH, use o executável Python que estiver configurado na sua máquina.

Observações:

- `playwright` instala a biblioteca Python.
- `playwright install chromium` baixa o navegador usado no fallback JS.
- O crawler continua funcionando sem Playwright quando `Renderizar JS` estiver desligado.
- Se `Renderizar JS` estiver ligado e o Playwright não estiver instalado, o crawler registra no log que o fallback JS não pôde ser usado.

## Verificação rápida

```bash
python -m py_compile crawler_tk.py
```

## Observações

- O programa cria diretórios de saída automaticamente em `wikis/`.
- No modo padrão, o crawler usa apenas HTTP com biblioteca padrão.
- Quando `render_js_content` estiver ativado, o projeto espera que `playwright` esteja instalado e que o navegador Chromium do Playwright já tenha sido baixado.
