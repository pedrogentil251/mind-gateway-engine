# PROTOCOLO UNIFICADO DE DESENVOLVIMENTO E ENGENHARIA DE SOFTWARE

## 1. Fluxo Obrigatório de Rastreabilidade
Nenhuma alteração relevante deve ser feita diretamente na branch `main` sem rastreabilidade:

    ISSUE -> BRANCH -> COMMITS -> PULL REQUEST -> REVIEW/VALIDAÇÃO -> MERGE -> DEPLOY

### Nomenclatura de Branches
- `feature/<id>-descricao-curta`
- `fix/<id>-descricao-curta`
- `refactor/<id>-descricao-curta`
- `chore/<id>-descricao-curta`

## 2. Proteção da Branch `main`
- Proibido `push --force` na branch `main`.
- Todo Pull Request deve referenciar a Issue (`Closes #<id>`).
- Nunca commitar credenciais, tokens ou arquivos `.env`.
