import sys
import itertools
import time

# input 데이터를 처리하기 쉽게 이중 리스트 형태로 만든다.
def load_transactions(path_to_data):
    Transactions = []
    with open(path_to_data, 'r') as f:
        for t in f:
            t = list(t.strip().split('\t'))
            Transactions.append(t)
    return Transactions


# itemset 내의 요소들을 처리하기 쉽게 ordering 한다.
def item_order(Transactions):
    order = []
    order_result = []

    for t in Transactions:
        for item in t:
            item = int(item)
            if item not in order:
                order.append(int(item))
    order.sort()

    for item in order:
        order_result.append(str(item))
    return order_result


# order를 토대로 트랜잭션 데이터 아이템셋들을 정렬한다.
def t_sort(Transactions, order):
    for t in Transactions:
        t.sort(key=lambda x: order.index(x))

'''prune과정에서 버려지는 set은 다음 candidate에서 prune할때 
downward closure에 따라 확인해야하므로 discarded라는 변수에 저장하자.'''
def get_freq(Ck, Transactions, min_sup, prev_discarded):
    # frequent itemset과 그에 대한 카운트를 L과 sup_cnt 리스트에 할당
    L = []
    sup_cnt = []
    # 이전 pruning에서 제거한 대상을 넣어놓고 다음 pruning에서 downward closure 할때 사용
    new_discarded = []

    k = len(prev_discarded.keys())

    for Ck_idx in range(len(Ck)):
        discarded = False
        # 이전에 가지치기 당했다면 k가 0보다 클 것이다.
        if k > 0:
            # prev_discarded 리스트의 마지막 요소가 바로 이전 discarded 요소일 것이므로 k 인덱스 접근
            for item in prev_discarded[k]:
                # discarded 요소가 현재 candidate에 존재한다면 이전에 버려진 것이다.
                if set(item).issubset(set(Ck[Ck_idx])):
                    discarded = True
                    break
        # 버려지지 않았으면 frequency 계산해서 L과 함께 저장
        if not discarded:
            cnt = get_cnt(Ck[Ck_idx], Transactions)
            # above min_sup한 요소들만 L에 추가
            if cnt / len(Transactions) >= min_sup:
                L.append(Ck[Ck_idx])
                sup_cnt.append(cnt)
            # min_sup보다 작으면 new_discarded에 넣고 다음 루프에서 prev_discard로 활용
            else:
                new_discarded.append(Ck[Ck_idx])
    return L, sup_cnt, new_discarded


# 전체 트랜잭션에서 포함된 개수를 측정하는 함수
def get_cnt(Ck, Transactions):
    cnt = 0
    for t_idx in range(len(Transactions)):
        if set(Ck).issubset(set(Transactions[t_idx])):
            cnt += 1
    return cnt

def self_join(itemsets, order):
    C = []
    # 동일 itemsets 인덱스를 조합 하나하나 루프 돌면서 join
    for idx1 in range(len(itemsets)):
        for idx2 in range(idx1+1, len(itemsets)):
            item_result = join_src(itemsets[idx1], itemsets[idx2], order)
            #결과값이 있다면 C에 append
            if len(item_result) > 0:
                C.append(item_result)
    return C
# 각아이템 조합한 값을 리턴하는 함수
def join_src(itemset1, itemset2, order):
    #itemset을 order대로 정렬
    itemset1.sort(key = lambda x: order.index(x))
    itemset2.sort(key = lambda x: order.index(x))

    # join 할 수 있는지 판단 -> 마지막 요소 외에는 다 같아야 한다.
    for idx in range(len(itemset1)-1):
        if itemset1[idx] != itemset2[idx]:
            return []
    # itemset2의 마지막 요소와 결합
    if order.index(itemset1[-1]) < order.index(itemset2[-1]):
        return itemset1 + [itemset2[-1]]

    return []

# itemset의 원소로 만들 수 있는 모든 부분 집합 만들기
# chain.from_iterable로 빠르게 powerset 리스트를 만들어준다.
def powerset(s):
    return list(itertools.chain.from_iterable(itertools.combinations(s,r) for r in range(1, len(s) +1 )))
# output 형식에 맞게 리턴
def gen_output(S, X_S, sup, Transactions, conf):
    output = ""
    output += "{}\t{}\t{:.2f}\t{:.2f}\n".format(set(S), set(X_S),sup/len(Transactions)*100,conf*100)
    return output

if __name__ == '__main__':
    start = time.time()
    min_sup = float(sys.argv[1]) / 100
    input_data = sys.argv[2]
    output_data = sys.argv[3]
    path_to_data = '/content/input.txt'

    Transactions = load_transactions(input_data)
    order = item_order(Transactions)
    t_sort(Transactions, order)

    C = {}
    L = {}
    # itemset의 길이를 기준으로 처리
    itemset_len = 1
    # 첫번째 itemset에 대한 discarded는 없으므로 빈 리스트 할당
    discarded = {itemset_len: []}
    # 첫번째 candidate는 order과 동일하다. 추후 처리를 쉽게 하기 위해 아이템을 리스트로 감싸준다.
    C.update({itemset_len: [[item] for item in order]})

    sup_cnt = {}
    # 첫번째 frequent set에 대한 데이터를 넣어준다.
    L_src, sup_cnt_src, discarded_src = get_freq(C[itemset_len], Transactions, min_sup, discarded)
    discarded.update({itemset_len: discarded_src})
    L.update({itemset_len: L_src})
    sup_cnt.update({itemset_len: sup_cnt_src})

    # candidate나 L으로 만들 새로운 길이 할당
    new_len = itemset_len + 1
    loop_break = False
    while not loop_break:
        # self-join은 k-1의 L을 가지고 Ck를 만든다. 여기서 k는 new_len
        C.update({new_len: self_join(L[new_len - 1], order)})
        L_src, sup_cnt_src, new_discarded = get_freq(C[new_len], Transactions, min_sup, discarded)
        # 각각의 새로 업데이트된 데이터를 맞는 변수에 update 해준다.
        L.update({new_len: L_src})
        sup_cnt.update({new_len: sup_cnt_src})
        discarded.update({new_len: new_discarded})

        if len(L[new_len]) < 1:
            loop_break = True

        new_len += 1
        # association rules output을 만드는 과정
        output = ""
        for i in range(1, len(L)):
            for j in range(len(L[i])):
                # L[i][j] 원소로 만들 수 있는 모든 부분 집합을 리스트에 넣는다.
                # str item도 iterate하는걸 방지하기 위해 set으로 감싸준다.
                s = list(powerset(set(L[i][j])))
                # 본인은 제외
                s.pop()
                # itemset과 associative itemset를 차집합을 이용해서 구한다.
                for z in s:
                    # 트랜잭션 데이터를 X에 할당
                    X = set(L[i][j])
                    # X -> Y에서 X에 해당하는 데이터를 S에 할당
                    S = set(z)
                    # Y에 해당하는 데이터는 차집합으로 구한다.
                    X_S = set(X - S)
                    # 각각의 support와 confidence를 구한다.
                    sup = get_cnt(X, Transactions)
                    conf = sup / get_cnt(S, Transactions)
                    #채점 item의 타입이 int이므로 int로 바꿔준다.
                    S = map(int, z)
                    X = map(int,L[i][j])
                    X_S = map(int, X_S)
                    # support 조건에 해당하면 출력한다.
                    if sup >= min_sup:
                        output += gen_output(S, X_S, sup, Transactions, conf)
        # argument[3]에 ouput 데이터를 넣어준다.
        with open(sys.argv[3], 'a') as f:
            f.write(output)
    print("time : ", time.time() - start)

