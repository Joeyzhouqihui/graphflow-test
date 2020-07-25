import heapq

if __name__ == "__main__":
    heap = []
    heapq.heappush(heap, 0)
    heapq.heappush(heap, -1)
    heapq.heappush(heap, 2)
    print(heap)
    print(heapq.nlargest(1, heap))