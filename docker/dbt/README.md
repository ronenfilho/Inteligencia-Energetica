# dbt Docker Setup

Este diretório contém a configuração Docker para executar o dbt (data build tool) com Snowflake.

## 📋 Pré-requisitos

- Docker e Docker Compose instalados
- Credenciais do Snowflake configuradas

## 🚀 Instalação

### 1. Configurar profiles

Copie o arquivo de exemplo e configure suas credenciais:

```bash
cd ../../dbt
cp profiles.exemplo.yml profiles.yml
```

Edite o arquivo `profiles.yml` com suas credenciais do Snowflake.

### 2. Tornar o script executável

```bash
chmod +x dbt_docker.sh
```

### 3. (Opcional) Configurar aliases

Para facilitar o uso, adicione os aliases ao seu shell:

```bash
echo "source $(pwd)/aliases.sh" >> ~/.bashrc
source ~/.bashrc
```

## 📦 Uso

### Comandos principais

```bash
# Executar modelos
./dbt_docker.sh run

# Executar testes
./dbt_docker.sh test

# Build completo (run + test)
./dbt_docker.sh build

# Verificar configuração
./dbt_docker.sh debug
```

### Com aliases (se configurado)

```bash
dbt_run
dbt_test
dbt_build
dbt_debug
```

### Comandos adicionais

```bash
# Instalar dependências
./dbt_docker.sh deps

# Compilar projeto
./dbt_docker.sh compile

# Carregar seeds
./dbt_docker.sh seed

# Gerar documentação
./dbt_docker.sh docs generate

# Servir documentação (acesse em http://localhost:8080)
./dbt_docker.sh docs serve

# Limpar arquivos temporários
./dbt_docker.sh clean

# Listar recursos
./dbt_docker.sh ls

# Abrir shell no container
./dbt_docker.sh shell
```

### Exemplos avançados

```bash
# Executar modelo específico
./dbt_docker.sh run --select nome_do_modelo

# Executar modelos com tag
./dbt_docker.sh run --select tag:staging

# Executar com full-refresh
./dbt_docker.sh run --full-refresh

# Testar apenas um modelo
./dbt_docker.sh test --select nome_do_modelo
```

## 📂 Estrutura

```
docker/dbt/
├── docker-compose.yml    # Configuração do Docker Compose
├── dbt_docker.sh        # Script principal com atalhos
├── aliases.sh           # Aliases para facilitar o uso
└── README.md            # Esta documentação
```

## 🔧 Configuração do Docker Compose

O `docker-compose.yml` está configurado para:

- Usar a imagem oficial `dbt-snowflake`
- Montar o diretório do projeto dbt em `/usr/app`
- Usar o arquivo `profiles.yml` do projeto

## 🐛 Troubleshooting

### Erro de conexão com Snowflake

1. Verifique se o `profiles.yml` está configurado corretamente
2. Execute `./dbt_docker.sh debug` para diagnosticar

### Dependências não encontradas

Execute:
```bash
./dbt_docker.sh deps
```

### Limpar cache

```bash
./dbt_docker.sh clean
docker-compose down -v
```

## 📚 Documentação

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Snowflake Profile](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile)

## 🎯 Fluxo de trabalho recomendado

1. **Desenvolvimento**
   ```bash
   ./dbt_docker.sh debug      # Verificar configuração
   ./dbt_docker.sh deps       # Instalar dependências
   ./dbt_docker.sh compile    # Compilar para ver SQL gerado
   ./dbt_docker.sh run        # Executar modelos
   ```

2. **Validação**
   ```bash
   ./dbt_docker.sh test       # Executar testes
   ./dbt_docker.sh build      # Build completo
   ```

3. **Documentação**
   ```bash
   ./dbt_docker.sh docs generate
   ./dbt_docker.sh docs serve
   ```
