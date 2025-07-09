from dataclasses import dataclass
import io
import struct

TAM_CAB_BUCKETS:int = 2 # Tamanho do cabeçalho do arquivo de buckets
TAM_MAX_BUCKET:int = 3
FORMAT_CAMPO:str = "I"
FORMAT_CAB:str = "H"
TAM_CAMPO:int = 4
ARQUIVO_BUCKETS:str = "buckets.dat"
TAM_REG_BUCKET = TAM_CAMPO*2 + TAM_MAX_BUCKET*TAM_CAMPO

@dataclass
class Diretorio:
    profundidade:int = 0
    buckets = []

class Bucket:
    def __init__(self,ref=None,profundidade=None,quant_chaves=None,chaves=None):
        if ref:
            self.ref = ref
        else:
            self.ref = 0

        if profundidade:
            self.profundidade = profundidade
        else:
            self.profundidade = 0
        
        if quant_chaves:
            self.quant_chaves = quant_chaves
        else:
            self.quant_chaves = 0
        
        if chaves:
            self.chaves = chaves
        else:
            self.chaves = [0,0,0]
        
    ref:int = 0
    profundidade:int = 0 # 4 bytes
    quant_chaves:int = 0 # 4 bytes
    chaves = [0]*TAM_MAX_BUCKET # 4 bytes * TAM_MAX_BUCKET

# Inicialização ====================================================================================================================
def inicializar_diretorio():
    dir:Diretorio = Diretorio() # Cria o diretório
    
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"wb+")
    arq.write(int.to_bytes(1,TAM_CAB_BUCKETS))
        
    novo_bucket:Bucket = Bucket()
    novo_bucket.profundidade = dir.profundidade
    novo_bucket.ref = 0
    escrever_bucket(novo_bucket)

    dir.buckets.append(0)
    arq.close()
    return dir

# Funções buckets ===============================================================================================================
def criar_bucket(dir:Diretorio):
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    cab = arq.read(TAM_CAB_BUCKETS)
    quant_buckets = 0
    if cab:
        quant_buckets = int.from_bytes(cab)    
        arq.seek((TAM_REG_BUCKET*quant_buckets)+TAM_CAB_BUCKETS)
    else:
        arq.write(int.to_bytes(0,TAM_CAB_BUCKETS))

    novo_bucket:Bucket = Bucket()
    novo_bucket.profundidade = dir.profundidade
    novo_bucket.ref = quant_buckets

    arq.write(int.to_bytes(novo_bucket.profundidade,TAM_CAMPO))
    arq.write(int.to_bytes(novo_bucket.quant_chaves,TAM_CAMPO))

    for _ in range(TAM_MAX_BUCKET):
        arq.write(int.to_bytes(0,TAM_CAMPO))

    arq.seek(0)
    arq.write(int.to_bytes(quant_buckets+1,TAM_CAB_BUCKETS))
    arq.close()
    return novo_bucket

def carrega_bucket(rrn:int):
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    bucket_carregado:Bucket = Bucket()
    
    posicao = (rrn*TAM_REG_BUCKET)+TAM_CAB_BUCKETS
    arq.seek(posicao)

    bucket_carregado.profundidade = int.from_bytes(arq.read(TAM_CAMPO))
    bucket_carregado.quant_chaves = int.from_bytes(arq.read(TAM_CAMPO))
    bucket_carregado.ref = rrn

    for i in range(bucket_carregado.quant_chaves):
        bucket_carregado.chaves[i] = int.from_bytes(arq.read(TAM_CAMPO))
    return bucket_carregado

def dividir_bucket(bucket:Bucket,diretorio:Diretorio):
    novo_bucket:Bucket = criar_bucket(diretorio)
    inicio, fim = encontrar_novo_intervalo(bucket,diretorio.profundidade)

    for i in range(inicio,fim+1):
        diretorio.buckets[i] = novo_bucket.ref

    bucket.profundidade += 1
    escrever_bucket(novo_bucket)
    escrever_bucket(bucket)

def buscar_chave_bucket(chave:int, bucket:Bucket):
    arq:io.TextIOBase = open(ARQUIVO_BUCKETS,"rb")
    arq.seek(posicao_bucket(bucket)+TAM_CAMPO*2)

    encontrado = int.from_bytes(arq.read(TAM_CAMPO))
    pos_chave = 0

    while encontrado != None and pos_chave < bucket.quant_chaves:
        if encontrado == chave:
            return encontrado, pos_chave # Chave, Foi encontrada?, posição onde encontrou
        else:
            pos_chave += 1
    return None, 0

def excluir_chave_bucket(chave:int, bucket:Bucket):
    chave_encontrada, pos = buscar_chave_bucket(chave,bucket)
    if chave_encontrada != None:
        bucket.chaves[pos] = 0
        bucket.quant_chaves -= 1
        deslocar_chaves(bucket.chaves,pos)
        escrever_bucket(bucket)

def posicao_bucket(bucket:Bucket):
    return TAM_REG_BUCKET*bucket.ref+TAM_CAB_BUCKETS

def deslocar_chaves(chaves:list[int],posicao:int):
    for i in range(posicao,len(chaves)-1):
        chaves[i] = chaves[i+1]

def escrever_bucket(bucket:Bucket):
    buckets:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    pos = posicao_bucket(bucket)
    buckets.seek(pos)
    buckets.write(int.to_bytes(bucket.profundidade,TAM_CAMPO))
    buckets.write(int.to_bytes(bucket.quant_chaves,TAM_CAMPO))

    for i in range(TAM_MAX_BUCKET):
        buckets.write(int.to_bytes(bucket.chaves[i],TAM_CAMPO))
    buckets.close()

# Funções diretório ==========================================================================================================
def expandir_diretorio(dir:Diretorio):
    novos_buckets = []

    for i in range(len(dir.buckets)):
        novos_buckets += [dir.buckets[i]]*2
    dir.buckets = novos_buckets
    dir.profundidade+=1

def buscar_chave_diretorio(chave:int,diretorio:Diretorio):
    endereco = gerar_endereco(chave,diretorio.profundidade)
    bucket = carrega_bucket(diretorio.buckets[endereco])
    encontrada = buscar_chave_bucket(chave,bucket)
    return encontrada, bucket

def printar_diretorio(dir:Diretorio):
    print("---- Diretório ----")
    for bucket in dir.buckets:
        bk = carrega_bucket(bucket)
        print(f"dir[{bucket}] -> bucket{bk.ref}:{bk.chaves}")

# Funções de inserção =============================================================================================================
def gerar_endereco(chave:int,profundidade:int):
    val_ret = 0
    mascara = 1
    hash_val = hash(chave)

    for _ in range(profundidade):
        val_ret = val_ret << 1
        bit_baixa_ordem = hash_val & mascara # Ultimo bit do hash
        val_ret = val_ret | bit_baixa_ordem # Adiciona o ultimo bit
        hash_val = hash_val >> 1 # Passa para o próximo bit
    return val_ret

def inserir_chave(chave:int,dir:Diretorio):
    #if buscar_chave_diretorio(chave,dir) != None: return False # Chave duplicada

    end = gerar_endereco(chave,dir.profundidade)
    bucket:Bucket = carrega_bucket(dir.buckets[end])

    if bucket.quant_chaves < TAM_MAX_BUCKET: # Insere
        bucket.chaves[bucket.quant_chaves] = chave
        bucket.quant_chaves += 1
        escrever_bucket(bucket)
    else:
        if bucket.profundidade >= dir.profundidade:
            expandir_diretorio(dir)
            inserir_chave(chave,dir)
        else:
            dividir_bucket(bucket,dir)
            redistribuir_chaves(bucket,dir)
            inserir_chave(chave,dir)
    return True

def redistribuir_chaves(bucket:Bucket,diretorio:Diretorio):
    chaves = bucket.chaves.copy()
    for i in range(len(chaves)):
        excluir_chave_bucket(chaves[i],bucket) # Esvazia o bucket
    for chave in chaves:
        inserir_chave(chave,diretorio) # Redistribui

def encontrar_novo_intervalo(bucket:Bucket,dir_prof:int):
    mascara = 1
    chave = bucket.chaves[0]
    end_comum = gerar_endereco(chave, bucket.profundidade)
    end_comum = end_comum << 1
    end_comum = end_comum | mascara
    bits_a_preencher = dir_prof - (bucket.profundidade + 1)
    novo_inicio, novo_fim = end_comum, end_comum
    for _ in range(bits_a_preencher):
        novo_inicio = novo_inicio << 1
        novo_fim = novo_fim << 1
        novo_fim = novo_fim | mascara

    return novo_inicio, novo_fim

def main():
    dir = inicializar_diretorio()

    inserir_chave(1,dir)
    inserir_chave(2,dir)
    inserir_chave(3,dir)
    inserir_chave(4,dir)
    printar_diretorio(dir)
    


if __name__ == "__main__":
    main()