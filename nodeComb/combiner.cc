#include "combiner.hh"

using namespace std;

Combiner::Combiner(NodeInfo *info, int nv, int dn, int fn, int r) : nodeInfo(info), combineTo(dn), fileNum(fn), replicaNum(r) {
    ubfactor = 1.25;
    balEIFactor = 0.9;
    netTpEIFactor = 0.8;
    iterNum = 10;
    assert(info != NULL);
    graph = new Graph(nv);
    GenerateStdFileDist();
    ConvertNodeInfo2Graph();
    CalculateTopKStdDist();
}

Combiner::~Combiner() {
    delete graph;
}

void Combiner::GenerateStdFileDist() {
    stdFileDist.resize(combineTo);
    // generate subset of nodes ( K choose r )
    vector<set<int> > list;
    vector<set<int> > nodeSubsets;
    list.push_back(set<int>());
    for(int i = 0; i < combineTo; i++) {
        int listNum = list.size();
        for(int j = 0; j < listNum; j++) {
            set<int> tmpList(list[j]);
            tmpList.insert(i);
            if(tmpList.size() == replicaNum) {
                nodeSubsets.push_back(tmpList);
            } else {
                list.push_back(tmpList);
            }
        }
    }
    assert(fileNum % nodeSubsets.size() == 0);
    int eta = fileNum / nodeSubsets.size();
    int fileId = 0;
    for(int i = 0; i < nodeSubsets.size(); i++) {
        for(int j = 0; j < eta; j++) {
            for(int nid : nodeSubsets[i]) {
                stdFileDist[nid].push_back(fileId);
            }
            fileId++;
        }
    }
    return ;
}

int Combiner::Dot(string &a, string &b) {
    int res = 0;
    assert(a.size() == b.size());
    for(int i = 0; i < a.size(); i++) {
        res += (a[i] - '0') * (b[i] - '0');
    }
    return res;
}

double Combiner::GetVectorLength(string &s) {
    int res = 0;
    for(int i = 0; i < s.size(); i++) {
        res += pow((s[i] - '0'), 2);
    }
    return sqrt(1.0 * res);
}

double Combiner::GetSimilarity(const vector<int> &nodeX, const vector<int> &nodeY) {
    double maxSimilarity = 0;
    string a = string(fileNum, '0');
    string b = string(fileNum, '0');
    for(int fid : nodeX) {
        a[fid] = '1';
    }
    for(int fid : nodeY) {
        a[fid] = '1';
    }
    for(int i = 0; i < combineTo; i++) {
        for(int fid : stdFileDist[i]) {
            b[fid] = '1';
        }
        maxSimilarity = max(maxSimilarity, (Dot(a, b) * 1.0) / (GetVectorLength(a) * GetVectorLength(b)));
    }
    return maxSimilarity;
}

double Combiner::GetSimilarityForVtx(int vtx, const vector<int> &nodeX, const vector<int> &nodeY) {
    double maxSimilarity = 0;
    string a = string(fileNum, '0');
    string b = string(fileNum, '0');
    for(int fid : nodeX) {
        a[fid] = '1';
    }
    for(int fid : nodeY) {
        a[fid] = '1';
    }
    vector<int> &topDistIdxs = topSimStdFileDist[vtx];
    for(int i = 0; i < topDistIdxs.size(); i++) {
        for(int fid : stdFileDist[topDistIdxs[i]]) {
            b[fid] = '1';
        }
        maxSimilarity = max(maxSimilarity, (Dot(a, b) * 1.0) / (GetVectorLength(a) * GetVectorLength(b)));
    }
    return maxSimilarity;
}

void Combiner::ConvertNodeInfo2Graph() {
    assert(nodeInfo->fileDistribution.size() == graph->nvtxs);
    vector<vector<int>> &blockInfo = nodeInfo->fileDistribution;
    // network topology
    for(int i = 0; i < graph->nvtxs; i++) {
        graph->vracks[i].push_back(make_pair(nodeInfo->networkTopology[i], 1));
        graph->topKRacks[i].insert(nodeInfo->networkTopology[i]);
    }
    for(int i = 0; i < graph->nvtxs; i++) {
        graph->vwgt.push_back(blockInfo[i].size());
        graph->tvwgt += blockInfo[i].size();
    }
    for(int i = 0; i < graph->nvtxs; i++) {
        graph->xadj.push_back(graph->adjncy.size());
        for(int j = 0; j < graph->nvtxs; j++) {
            if(j == i) {
                continue;
            }
            // wgt = similarity * average_vwgt / (vwgt[i] + vwgt[j])
            double wgt = GetSimilarity(blockInfo[i], blockInfo[j]) * 100 * 2 * graph->tvwgt / graph->nvtxs / (blockInfo[i].size() + blockInfo[j].size()); // 保留两位小数
            // considering network topology, wgt = original_wgt / ((1 - netTpEIFactor) * 2 * #(set_union(topKRacks[i], topKRacks[j])) / (#(topKRacks[i]) + #(topKRacks[j])))
            set<int> unionSet;
            set_union(graph->topKRacks[i].begin(), graph->topKRacks[i].end(),
                             graph->topKRacks[j].begin(), graph->topKRacks[j].end(),
                             inserter(unionSet, unionSet.begin()));
            // wgt /= (1 - netTpEIFactor) * 2 * unionSet.size() / (graph->topKRacks[i].size() + graph->topKRacks[j].size());
            wgt /= netTpEIFactor * 2 * pow(unionSet.size(), 2) / pow(graph->topKRacks[i].size() + graph->topKRacks[j].size(), 2);
            if(wgt != 0) { // drop edges = off
                graph->adjncy.push_back(j);
                graph->adjwgt.push_back(wgt);
            }
        }
    }
    graph->xadj.push_back(graph->adjncy.size());
    graph->invtvwgt = 1.0 / graph->tvwgt;
    graph->nedges = graph->adjncy.size() / 2;
    graph->orgvwgt = graph->vwgt;
    
    return;
}

void Combiner::CalculateTopKStdDist() {
    topSimStdFileDist.resize(graph->nvtxs);
    // int k = combineTo / 2;
    int k = 2;
    vector<string> stdDistVec(combineTo);
    for(int i = 0; i < combineTo; i++) {
        string a = string(fileNum, '0');
        for(int fid : stdFileDist[i]) {
            a[fid] = '1';
        }
        stdDistVec[i] = a;
    }
    for(int i = 0; i < graph->nvtxs; i++) {
        vector<pair<int, double>> sims;
        string a = string(fileNum, '0');
        for(int fid : nodeInfo->fileDistribution[i]) {
            a[fid] = '1';
        }
        for(int j = 0; j < combineTo; j++) {
            double sim = (Dot(a, stdDistVec[j]) * 1.0) / (GetVectorLength(a) * GetVectorLength(stdDistVec[j]));
            sims.push_back(make_pair(j, sim));
        }
        sort(sims.begin(), sims.end(), [](const auto &a, const auto &b) {
            return a.second > b.second;
        });
        for(int j = 0; j < k; j++) {
            topSimStdFileDist[i].push_back(sims[j].first);
        }
    }
    // cout << "topSimStdFileDist:" << endl;
    // for(int i = 0; i < graph->nvtxs; i++) {
    //     cout << i << ": ";
    //     for(int j = 0; j < k; j++) {
    //         cout << topSimStdFileDist[i][j] << " ";
    //     }
    //     cout << endl;
    // }
    return ;
}

void Combiner::GetCombinationResult(vector<int> &npart) {
    double curUbVariance, bestUbVariance;
    int curCodeNum, bestCodeNum;
    for(int i = 0; i < iterNum; i++) {
        int coarsenRound = log((0.0 + graph->orgNvtxs) / combineTo) / log(2.0);
        if(int(pow(2.0, coarsenRound)) * combineTo == graph->orgNvtxs) {
            coarsenRound--;
        }
        Graph *curGraph = graph;
        for(int round = 0; round < coarsenRound; round++) {
            // coarse by heavy edge matching
            GreedyMatch(curGraph);
            curGraph = curGraph->cgraph;
            // cout << "finish matching, now nvtxs: " << curGraph->nvtxs << endl;
        }
        GreedyMatchForDestStrict(curGraph);
        while(curGraph->cgraph && curGraph->cgraph->cgraph) {
            curGraph = curGraph->cgraph;
        }
        CalculateCombinationQuality(curGraph->cgraph, curUbVariance, curCodeNum);
        // cout << "round: " << i + 1 << ", unbalance variance = " << curUbVariance << ", codeNum = " << curCodeNum << endl;
        if(i == 0 || curCodeNum > bestCodeNum || (curCodeNum == bestCodeNum && curUbVariance < bestUbVariance)) {
            npart = curGraph->cgraph->where;
            bestCodeNum = curCodeNum;
            bestUbVariance = curUbVariance;
        }
        DeleteCoarsenGraph();
        graph->cmap.resize(graph->nvtxs);
    }

    return;
}

// heavy edge matching
void Combiner::GreedyMatch(Graph *curGraph) {
    ubfactor = 1.25;
    vector<int> match(curGraph->nvtxs, UNMATCHED);
    double maxvwgt = ubfactor * graph->tvwgt / (curGraph->nvtxs / 2);
    // generate a random traversal order
    vector<int> perm(curGraph->nvtxs);
    iota(perm.begin(), perm.end(), 0);
    random_device rd;
    default_random_engine rng(rd());
    shuffle(perm.begin(), perm.end(), rng);
    // Traverse the vertices and compute the matching
    int cnvtxs, lastUnmatched, pi, i, j, k, maxIdx, maxWgt;
    for(cnvtxs = 0, lastUnmatched = 0, pi = 0; pi < curGraph->nvtxs; pi++) {
        i = perm[pi];
        if(match[i] == UNMATCHED) {
            maxIdx = i;
            maxWgt = -1;
            if(curGraph->vwgt[i] < maxvwgt) {
                // Deal with island vertices. Find a non-island and match it with. The matching ignores ctrl->maxvwgt requirements
                if (curGraph->xadj[i] == curGraph->xadj[i+1]) { 
                    lastUnmatched = max(pi, lastUnmatched) + 1;
                    for ( ; lastUnmatched < curGraph->nvtxs; lastUnmatched++) {
                        j = perm[lastUnmatched];
                        if (match[j] == UNMATCHED) {
                            maxIdx = j;
                            break;
                        }
                    }
                } else {
                    // Find a heavy-edge matching, subject to maxvwgt constraints
                    for(j = curGraph->xadj[i]; j < curGraph->xadj[i + 1]; j++) {
                        k = curGraph->adjncy[j];
                        if (match[k] == UNMATCHED && maxWgt < curGraph->adjwgt[j] && curGraph->vwgt[i] + curGraph->vwgt[k] <= maxvwgt) {
                            maxIdx = k;
                            maxWgt = curGraph->adjwgt[j];
                        }
                    }
                    // if (maxIdx == i && 2 * curGraph->vwgt[i] < maxvwgt) {
                    if (maxIdx == i) {
                        maxIdx = UNMATCHED;
                    }
                }
            }
            if (maxIdx != UNMATCHED) {
                curGraph->cmap[i]  = curGraph->cmap[maxIdx] = cnvtxs++;
                match[i] = maxIdx;
                match[maxIdx] = i;
            }
        }
    }
    // match the final unmatched vertices with themselves and reorder the vertices of the coarse graph for memory-friendly contraction
    for (cnvtxs = 0, i = 0; i < curGraph->nvtxs; i++) {
        if (match[i] == UNMATCHED) {
            match[i] = i;
            curGraph->cmap[i] = cnvtxs++;
        }
        else {
            if (i <= match[i]) {
                curGraph->cmap[i] = curGraph->cmap[match[i]] = cnvtxs++;
            }
        }
    }

    CreateCoarsenGraph(curGraph, cnvtxs, match);

    Graph *g = curGraph->cgraph;
    ComputeKWayRefineParams(g, balEIFactor);
    ComputeKWayBoundary(g, OMODE_BALANCE);
    GreedyKWayOptimize(g, 1, OMODE_BALANCE);
    ComputeKWayRefineParams(g, 1);
    ComputeKWayBoundary(g, OMODE_REFINE);
    GreedyKWayOptimize(g, iterNum, OMODE_REFINE);
    // ComputeKWayBoundary(g, OMODE_NETWORK_TOPOLOGY);
    // GreedyKWayOptimize(g, 1, OMODE_NETWORK_TOPOLOGY);
    UpdateAdjacencyEdges(g);

    return ;
}

void Combiner::GreedyMatchForDestStrict(Graph *curGraph) {
    if(!(curGraph->nvtxs > combineTo && curGraph->nvtxs <= 2 * combineTo)) {
        while(curGraph->nvtxs > 2 * combineTo) {
            GreedyMatch(curGraph);
            curGraph = curGraph->cgraph;
        }
    }
    assert(curGraph->nvtxs > combineTo && curGraph->nvtxs <= 2 * combineTo);
    double maxvwgt = ubfactor * graph->tvwgt / combineTo;
    int numToMatch = curGraph->nvtxs - combineTo;
    vector<int> match(curGraph->nvtxs, UNMATCHED);
    vector<pair<int, pair<int, int>>> edges; // (wgt, (u, v)) u < v
    for(int u = 0; u < curGraph->nvtxs; u++) {
        for(int i = curGraph->xadj[u]; i < curGraph->xadj[u + 1]; i++) {
            int v = curGraph->adjncy[i];
            if(u > v) {
                continue;
            }
            edges.push_back(make_pair(curGraph->adjwgt[i], make_pair(u, v)));
        }
    }
    sort(edges.begin(), edges.end(),  [](const auto& a, const auto& b) {
        return a.first > b.first;
    });

    int cnvtxs = 0, i;
    for(const auto &edge : edges) {
        int u = edge.second.first, v = edge.second.second;
        // if(match[u] != UNMATCHED || match[v] != UNMATCHED || curGraph->vwgt[u] + curGraph->vwgt[v] > maxvwgt) {
        if(match[u] != UNMATCHED || match[v] != UNMATCHED) {
            continue;
        }
        curGraph->cmap[u] = curGraph->cmap[v] = cnvtxs++;
        match[u] = v;
        match[v] = u;
        if(--numToMatch == 0) {
            break;
        }
    }
    if(numToMatch) {
        for(const auto &edge : edges) {
            int u = edge.second.first, v = edge.second.second;
            if(match[u] != UNMATCHED || match[v] != UNMATCHED) {
                continue;
            }
            curGraph->cmap[u] = curGraph->cmap[v] = cnvtxs++;
            match[u] = v;
            match[v] = u;
            if(--numToMatch == 0) {
                break;
            }
        }
    }
    // match the final unmatched vertices with themselves and reorder the vertices of the coarse graph for memory-friendly contraction
    for (cnvtxs = 0, i = 0; i < curGraph->nvtxs; i++) {
        if (match[i] == UNMATCHED) {
            match[i] = i;
            curGraph->cmap[i] = cnvtxs++;
        }
        else {
            if (i <= match[i]) {
                curGraph->cmap[i] = curGraph->cmap[match[i]] = cnvtxs++;
            }
        }
    }

    assert(cnvtxs == combineTo);

    CreateCoarsenGraph(curGraph, cnvtxs, match);

    ubfactor = 1.1;
    Graph *g = curGraph->cgraph;
    ComputeKWayRefineParams(g, balEIFactor);
    ComputeKWayBoundary(g, OMODE_BALANCE);
    GreedyKWayOptimize(g, iterNum, OMODE_BALANCE);
    ComputeKWayRefineParams(g, 1);
    ComputeKWayBoundary(g, OMODE_REFINE);
    GreedyKWayOptimize(g, iterNum * 2, OMODE_REFINE);
    ComputeKWayBoundary(g, OMODE_NETWORK_TOPOLOGY);
    AdjustSingleRackNode(g);
    // balEIFactor = 1;
    // ComputeKWayBoundary(g, OMODE_BALANCE);
    // GreedyKWayOptimize(g, iterNum, OMODE_BALANCE);
    // UpdateAdjacencyEdges(g);

    return ;
}

void Combiner::GreedyKWayOptimize(Graph *g, int niter, int omode) {
    vector<int> &where = g->where;
    vector<int> &vwgt = g->vwgt;

    double maxpwgts = ubfactor * g->tvwgt / g->nvtxs;
    double minpwgts = (1.0 / ubfactor) * g->tvwgt / g->nvtxs;
    double modeEIFactor = 1;
    int i, j, k, from, to, nmove, me;

    for(int iter = 0; iter < niter; iter++) {
        nmove = 0;
        if(omode == OMODE_BALANCE) {
            // Check to see if things are out of balance, given the tolerance
            for(i = 0; i < g->nvtxs; i++) {
                if(g->vwgt[i] > maxpwgts || g->vwgt[i] < minpwgts) {
                    break;
                }
            }
            if(i == g->nvtxs) {
                break;
            }
        }
        if(g->bndList.size() == 0) {
            break;
        }

        vector<pair<int, int>> bndvtxs; // naive simulation of priority_queue with update
        vector<int> vstatus(g->orgNvtxs, VPQSTATUS_NOTPRESENT);
        for(int vtx : g->bndList) {
            bndvtxs.push_back(make_pair(vtx, g->ckrinfo[vtx].ed - modeEIFactor * g->ckrinfo[vtx].id));
            vstatus[vtx] = VPQSTATUS_PRESENT;
        }
        sort(bndvtxs.begin(), bndvtxs.end(), [](const auto& a, const auto& b) {
            return a.second > b.second;
        });

        // Start extracting vertices and try to move them
        while(bndvtxs[0].second >= 0) {
            int i = bndvtxs[0].first;
            vstatus[i] = VPQSTATUS_EXTRACTED;
            KwayNbrInfo &myrinfo = g->ckrinfo[i];
            
            from = where[i];
            int wgt = g->orgvwgt[i], rackId = nodeInfo->networkTopology[i];
            // Find the most promising subdomain to move to
            if(omode == OMODE_REFINE) {
                for(to = 0; to < g->nvtxs; to++) {
                    if(to == from) {
                        continue;
                    }
                    if(g->topKRacks[to].find(rackId) != g->topKRacks[to].end() && 
                        (((myrinfo.nbrs[to] > myrinfo.id) &&
                          ((vwgt[from] - wgt >= minpwgts) || 
                           (vwgt[to] < vwgt[from] - wgt)) &&
                          ((vwgt[to] + wgt <= maxpwgts) || 
                           (vwgt[to] < vwgt[from] - wgt))
                         ) ||
                         ((myrinfo.nbrs[to] == myrinfo.id) &&
                          (vwgt[to] < vwgt[from] - wgt)))
                      ) {
                        break;
                    }
                }
                if(to == g->nvtxs) {
                    // break out if you did not find a candidate
                    bndvtxs[0].second = -1;
                    sort(bndvtxs.begin(), bndvtxs.end(), [](const auto& a, const auto& b) {
                        return a.second > b.second;
                    });
                    continue;
                }
                for(j = to + 1; j < g->nvtxs; j++) {
                    if(j == from) {
                        continue;
                    }
                    if(g->topKRacks[j].find(rackId) != g->topKRacks[j].end() && (((myrinfo.nbrs[j] > myrinfo.nbrs[to]) &&
                        ((vwgt[from] - wgt >= minpwgts) || 
                         (vwgt[j] < vwgt[from] - wgt)) && 
                        ((vwgt[j] + wgt <= maxpwgts) ||
                         (vwgt[j] < vwgt[from] - wgt))
                        ) || 
                        ((myrinfo.nbrs[j] == myrinfo.nbrs[to]) && 
                         (vwgt[j] < vwgt[to])))
                    ) {
                        to = j;
                    }
                }
                
            } else if(omode == OMODE_BALANCE) {
                // OMODE_BALANCE
                for(to = 0; to < g->nvtxs; to++) {
                    if(from == to) {
                        continue;
                    }
                    // if(from >= g->nvtxs || (g->topKRacks[to].find(rackId) != g->topKRacks[to].end() && balEIFactor * myrinfo.id <= myrinfo.nbrs[to] && vwgt[to] < vwgt[from] - wgt)) {
                    //     break;
                    // }
                    if(g->topKRacks[to].find(rackId) != g->topKRacks[to].end() && balEIFactor * myrinfo.id <= myrinfo.nbrs[to] && vwgt[to] < vwgt[from] - wgt) {
                        break;
                    }
                }
                if(to == g->nvtxs) {
                    // break out if you did not find a candidate
                    bndvtxs[0].second = -1;
                    sort(bndvtxs.begin(), bndvtxs.end(), [](const auto& a, const auto& b) {
                        return a.second > b.second;
                    });
                    continue;
                }
                for(j = to + 1; j < g->nvtxs; j++) {
                    if(j == from) {
                        continue;
                    }
                    if(g->topKRacks[j].find(rackId) != g->topKRacks[j].end() && balEIFactor * myrinfo.id <= myrinfo.nbrs[j] && vwgt[j] < vwgt[to]) {
                        to = j;
                    }
                }
            } else {
                // OMODE_NETWORK_TOPOLOGY
                int rackId = nodeInfo->networkTopology[i];
                for(to = 0; to < g->nvtxs; to++) {
                    if(from == to) {
                        continue;
                    }
                    if(from >= g->nvtxs || (g->topKRacks[to].find(rackId) != g->topKRacks[to].end() && netTpEIFactor * myrinfo.id <= myrinfo.nbrs[to] && vwgt[to] < vwgt[from] - wgt)) {
                        break;
                    }
                }
                if(to == g->nvtxs) {
                    // break out if you did not find a candidate
                    bndvtxs[0].second = -1;
                    sort(bndvtxs.begin(), bndvtxs.end(), [](const auto& a, const auto& b) {
                        return a.second > b.second;
                    });
                    continue;
                }
                int rackRankTo = 0, rackRankJ = 0;
                while(g->vracks[to][rackRankTo].first != rackId) {
                    rackRankTo++;
                }
                for(j = to + 1; j < g->nvtxs; j++) {
                    if(j == from) {
                        continue;
                    }
                    if(g->topKRacks[j].find(rackId) != g->topKRacks[j].end() && netTpEIFactor * myrinfo.id <= myrinfo.nbrs[j]) {
                        rackRankJ = 0;
                        while(g->vracks[j][rackRankJ].first != rackId) {
                            rackRankJ++;
                        }
                        if(myrinfo.nbrs[j] > myrinfo.nbrs[to] || rackRankJ < rackRankTo) {
                            to = j;
                            rackRankTo = rackRankJ;
                        }
                    }
                }
            }
            // int fromRackCnt = 0;
            // for(auto &it : g->vracks[from]) {
            //     if(it.first == rackId) {
            //         fromRackCnt = it.second;
            //         break;
            //     }
            // }
            // if(fromRackCnt <= 2) {
            //     // avoid single rack
            //     bndvtxs[0].second = -1;
            //     sort(bndvtxs.begin(), bndvtxs.end(), [](const auto& a, const auto& b) {
            //         return a.second > b.second;
            //     });
            //     continue;
            // }
            // If we got here, we can now move the vertex from 'from' to 'to'
            nmove++;
            // Update ID/ED and BND related information for the moved vertex
            vwgt[to] += wgt;
            vwgt[from] -= wgt;
            where[i] = to;
            myrinfo.id = myrinfo.nbrs[to];
            myrinfo.pNum = 0;
            myrinfo.ed = 0;
            for(int k = 0; k < g->nvtxs; k++) {
                if(k == to) {
                    continue;
                }
                if(g->topKRacks[k].find(rackId) != g->topKRacks[k].end() && myrinfo.nbrs[k] >= myrinfo.id) {
                    myrinfo.pNum++;
                    myrinfo.ed += myrinfo.nbrs[k];
                }
            }
            if(myrinfo.pNum) {
                myrinfo.ed /= myrinfo.pNum;
            }
            if(g->bndList.find(i) != g->bndList.end() && modeEIFactor * myrinfo.id >= myrinfo.ed) {
                g->bndList.erase(i);
            } else if(g->bndList.find(i) == g->bndList.end() && modeEIFactor * myrinfo.id < myrinfo.ed) {
                g->bndList.insert(i);
            }

            // update network topology (vRacks) of 'from' and 'to'
            int idx = 0;
            for(; idx < g->vracks[from].size(); idx++) {
                if(g->vracks[from][idx].first == rackId) {
                    break;
                }
            }
            int delIdx = idx;
            while(idx < g->vracks[from].size() && g->vracks[from][idx].second == g->vracks[from][delIdx].second) {
                idx++;
            }
            idx--;
            swap(g->vracks[from][idx], g->vracks[from][delIdx]);
            g->vracks[from][idx].second--;
            if(g->vracks[from].back().second == 0) {
                g->vracks[from].pop_back();
            }
            if(delIdx < g->kOfRack && idx >= g->kOfRack) {
                g->topKRacks[from].erase(rackId);
                g->topKRacks[from].insert(g->vracks[from][delIdx].first);
            }

            idx = 0;
            for(; idx < g->vracks[to].size(); idx++) {
                if(g->vracks[to][idx].first == rackId) {
                    break;
                }
            }
            if(idx < g->vracks[to].size()) {
                delIdx = idx;
                while(idx >= 0 && g->vracks[to][idx].second == g->vracks[to][delIdx].second) {
                    idx--;
                }
                idx++;
                swap(g->vracks[to][idx], g->vracks[to][delIdx]);
                g->vracks[to][idx].second++;
                if(idx < g->kOfRack && delIdx >= g->kOfRack) {
                    g->topKRacks[to].erase(g->vracks[to][idx].first);
                    g->topKRacks[to].insert(rackId);
                }
            } else {
                // rackId not exist in rackList[to]
                g->vracks[to].push_back(make_pair(rackId, 1));
                if(g->vracks[to].size() <= g->kOfRack) {
                    g->topKRacks[to].insert(rackId);
                }
            }
            

            // Update the degrees of other vertices
            UpdateVertexInfoAndBND(g, i, from, to, omode);
            // update bndvtxs 
            bndvtxs[0].second = -1;
            for(auto & it : bndvtxs) {
                int vtx = it.first;
                if(vstatus[vtx] == VPQSTATUS_PRESENT) {
                    it.second = g->ckrinfo[vtx].ed - modeEIFactor * g->ckrinfo[vtx].id;
                }
            }
            for(j = 0; j < g->orgNvtxs; j++) {
                int rackId = nodeInfo->networkTopology[j], vid = where[j];
                if(vstatus[j] == VPQSTATUS_NOTPRESENT && g->ckrinfo[j].ed - modeEIFactor * g->ckrinfo[j].id >= 0 && (omode != OMODE_NETWORK_TOPOLOGY || g->topKRacks[vid].find(rackId) == g->topKRacks[vid].end())) {
                    bndvtxs.push_back(make_pair(j, g->ckrinfo[j].ed - modeEIFactor * g->ckrinfo[j].id));
                }
            }
            sort(bndvtxs.begin(), bndvtxs.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });
        }
        if(nmove == 0) {
            break;
        }
    }

    return ;
}

void Combiner::CreateCoarsenGraph(Graph *curGraph, int cnvtxs, const vector<int> &match) {
    // Initialize the coarser graph
    Graph *corGraph = new Graph(cnvtxs, curGraph);
    curGraph->cgraph = corGraph;
    corGraph->vwgt.resize(cnvtxs);
    vector<int> &cvwgt = corGraph->vwgt;

    int u, v;

    // take care of the vertices
    for(v = 0, cnvtxs = 0; v < curGraph->nvtxs; v++) {
        if((u = match[v]) < v) {
            continue;
        }
        assert(curGraph->cmap[v] == cnvtxs);
        assert(curGraph->cmap[match[v]] == cnvtxs);
        cvwgt[cnvtxs] = curGraph->vwgt[v];
        if(v != u) {
            cvwgt[cnvtxs] += curGraph->vwgt[u];
        }
        cnvtxs++;
    }
    vector<vector<int>> corFileDist(cnvtxs);
    vector<vector<int>> &blockInfo = nodeInfo->fileDistribution;
    // take care of cwhere
    vector<int> &cwhere = corGraph->where;
    for(int i = 0; i < curGraph->orgNvtxs; i++) {
        cwhere[i] = curGraph->cmap[curGraph->where[i]];
        corFileDist[cwhere[i]].insert(corFileDist[cwhere[i]].end(), blockInfo[i].begin(), blockInfo[i].end());
    }

    // take care of the network topology
    const vector<int> &networkTopology = nodeInfo->networkTopology;
    vector<vector<pair<int, int>>> &vracks = corGraph->vracks;
    vector<set<int>> &topKRacks = corGraph->topKRacks;
    int k = corGraph->kOfRack;
    vector<map<int, int>> netTp(cnvtxs); // corNode -> (rackId, cnt)
    for(int i = 0; i < curGraph->orgNvtxs; i++) {
        int cvid = cwhere[i], rackId = networkTopology[i];
        netTp[cvid][rackId]++;
    }
    for(int i = 0; i < cnvtxs; i++) {
        // vector<pair<int, int>> racks(netTp[i].begin(), netTp[i].end());
        // sort(racks.begin(), racks.end(), [](const pair<int, int> &a, const pair<int, int> &b) {
        //     return a.second > b.second;
        // });
        // vracks[i] = racks;
        for(pair<int, int> it : netTp[i]) {
            vracks[i].push_back(it);
        }
        sort(vracks[i].begin(), vracks[i].end(), [](const pair<int, int> &a, const pair<int, int> &b) {
            return a.second > b.second;
        });
        for(int j = 0; j < k && j < vracks[i].size(); j++) {
            topKRacks[i].insert(vracks[i][j].first);
        }
    }
    // take care of the edges
    // weight of new edge = the similarity between new nodes
    vector<int> &cxadj = corGraph->xadj;
    vector<int> &cadjncy = corGraph->adjncy;
    vector<int> &cadjwgt = corGraph->adjwgt;
    for(int i = 0; i < cnvtxs; i++) {
        cxadj.push_back(cadjncy.size());
        for(int j = 0; j < cnvtxs; j++) {
            if(j == i) {
                continue;
            }
            // wgt = similarity * average_vwgt / (vwgt[i] + vwgt[j])
            int wgt = GetSimilarity(corFileDist[i], corFileDist[j]) * 100 * 2 * corGraph->tvwgt / cnvtxs / (cvwgt[i] + cvwgt[j]);
            // considering network topology, wgt = original_wgt / ((1 - netTpEIFactor) * 2 * #(set_union(topKRacks[i], topKRacks[j])) / (#(topKRacks[i]) + #(topKRacks[j])))
            set<int> unionSet, rackSetA, rackSetB;
            for(auto it : vracks[i]) {
                rackSetA.insert(it.first);
            }
            for(auto it : vracks[j]) {
                rackSetB.insert(it.first);
            }
            set_union(rackSetA.begin(), rackSetA.end(),
                             rackSetB.begin(), rackSetB.end(),
                             inserter(unionSet, unionSet.begin()));
            // wgt /= (1 - netTpEIFactor) * 2 * unionSet.size() / (topKRacks[i].size() + topKRacks[j].size());
            if(unionSet.size() > corGraph->maxRackNum) {
                wgt = 0;
            } else {
                wgt /= netTpEIFactor * 2 * pow(unionSet.size(), 2.0) / pow(rackSetA.size() + rackSetB.size(), 2);
            }
            if(wgt != 0) { // drop edges = off
                cadjncy.push_back(j);
                cadjwgt.push_back(wgt);
            }
        }
    }
    cxadj.push_back(cadjncy.size());
    corGraph->nedges = cadjncy.size() / 2;

    return ;
}

void Combiner::CalculateCombinationQuality(Graph *g, double &ubVariance, int &codeNum) {
    // calculate the variance of unbalance
    double avgvwgt = (0.0 + g->tvwgt) / g->nvtxs;
    codeNum = 0;
    ubVariance = 0;
    for(int i = 0; i < g->nvtxs; i++) {
        ubVariance += (avgvwgt - g->vwgt[i]) * (avgvwgt - g->vwgt[i]);
    }
    ubVariance /= g->nvtxs;
    // calculate the number of groups that can be coded
    vector<set<int>> fileDist(fileNum); // file id -> node id set
    vector<int> &where = g->where;
    vector<vector<int>> &blockInfo = nodeInfo->fileDistribution;
    for(int i = 0; i < g->orgNvtxs; i++) {
        for(int fileId : blockInfo[i]) {
            fileDist[fileId].insert(where[i] + 1);
        }
    }
    map<NodeSet, vector<int>> sameFileTab;
    for(int i = 0; i < fileDist.size(); i++) {
        sameFileTab[fileDist[i]].push_back(i);
    }
    vector<NodeSet> nodeSubsetR, nodeSubsetS;
    GenerateNodeSubset(replicaNum, nodeSubsetR);
    GenerateNodeSubset(replicaNum + 1, nodeSubsetS);
    for(const auto & it : nodeSubsetS) {
        int minFileNum = INT_MAX;
        bool canBeCoded = true;
        for(int nodeId : it) {
            set<int> subsetR(it);
            subsetR.erase(nodeId);
            if(sameFileTab.find(subsetR) == sameFileTab.end()) {
                canBeCoded = false;
                break;
            }
            minFileNum = min(minFileNum, (int)sameFileTab[subsetR].size());
        }
        if(canBeCoded) {
            codeNum += minFileNum;
        }
    }
    return ;
}

void Combiner::GenerateNodeSubset(int r, vector<NodeSet> &nodeSubset) {
    vector<NodeSet> list;
    list.push_back(NodeSet());
    for(int i = 1; i <= combineTo; i++) {
        unsigned int numList = list.size();
        for (unsigned int j = 0; j < numList; j++) {
            NodeSet n(list[j]);
            n.insert(i);
            if (int(n.size()) == r) {
                nodeSubset.push_back(n);
            } else {
                list.push_back(n);
            }
        }
    }
    return ;
}

// ed 定义为该点与其他逻辑节点合并而成的节点与标准分布的余弦相似度
// id 定义为该点当前所在逻辑节点与标准分布的余弦相似度
void Combiner::ComputeKWayRefineParams(Graph *g, double modeEIFactor) {
    vector<KwayNbrInfo> &ckrinfo = g->ckrinfo;
    vector<int> &orgvwgt = g->orgvwgt;
    vector<int> &where = g->where;
    vector<vector<int>> &blockInfo = nodeInfo->fileDistribution;
    int i, j, k, me;
    
    // Compute the required info for refinement
    vector<vector<int>> fileDist(g->nvtxs);
    for(int i = 0; i < g->orgNvtxs; i++) {
        fileDist[where[i]].insert(fileDist[where[i]].end(), blockInfo[i].begin(), blockInfo[i].end());
    }

    for(i = 0; i < g->orgNvtxs; i++) {
        me = where[i];
        KwayNbrInfo &myrinfo = g->ckrinfo[i];
        myrinfo.ed = 0;
        myrinfo.nbrs.resize(g->nvtxs);
        for(j = 0; j < g->nvtxs; j++) {
            int wgt;
            if(j == me) {
                // wgt = similarity * average_vwgt / vwgt[j]
                wgt = GetSimilarityForVtx(i, blockInfo[i], fileDist[j]) * 100 * g->tvwgt / g->nvtxs / fileDist[j].size();
                myrinfo.id = wgt;
            } else {
                // wgt = similarity * average_vwgt / (vwgt[i] + vwgt[j])
                wgt = GetSimilarityForVtx(i, blockInfo[i], fileDist[j]) * 100 * g->tvwgt / g->nvtxs / (blockInfo[i].size() + fileDist[j].size());
            }
            myrinfo.nbrs[j] = wgt;
        }
        int pNum = 0, rackId = nodeInfo->networkTopology[i];
        for(j = 0; j < g->nvtxs; j++) {
            if(j == me) {
                continue;
            }
            if(g->topKRacks[j].find(rackId) != g->topKRacks[j].end() && myrinfo.nbrs[j] >= myrinfo.id * modeEIFactor) {
                pNum++;
                myrinfo.ed += myrinfo.nbrs[j];
            }
        }
        myrinfo.pNum = pNum;
        if(pNum) {
            myrinfo.ed /= pNum;
        }
    }

    return;
}

void Combiner::ComputeKWayBoundary(Graph *g, int omode) {
    set<int> &bndList = g->bndList;
    bndList.clear();
    if(omode == OMODE_REFINE) {
        for(int i = 0; i < g->orgNvtxs; i++) {
            if(g->ckrinfo[i].ed - g->ckrinfo[i].id >= 0) {
                bndList.insert(i);
            }
        }
    } else if(omode == OMODE_BALANCE) {
        for(int i = 0; i < g->orgNvtxs; i++) {
            if(g->ckrinfo[i].id <= g->ckrinfo[i].ed) { // 为了进行点权和调整，ed 略小于 id 也看做边界节点
                bndList.insert(i);
            }
        }
    } else {
        for(int i = 0; i < g->orgNvtxs; i++) {
            int rackId = nodeInfo->networkTopology[i], vid = g->where[i];
            const vector<pair<int, int>> &racks = g->vracks[vid];
            for(auto it : racks) {
                if(it.first == rackId) {
                    if(it.second == 1) {
                        bndList.insert(i);
                    }
                    break;
                }
            }
            // if(g->topKRacks[vid].find(rackId) == g->topKRacks[vid].end() && netTpEIFactor * g->ckrinfo[i].id <= g->ckrinfo[i].ed) {
            //     bndList.insert(i);
            // } 
        }
    }
}

void Combiner::UpdateVertexInfoAndBND(Graph *g, int movedVtx, int from, int to, int omode) {
    set<int> &bndList = g->bndList;
    vector<int> fileDistFrom, fileDistTo;
    vector<int> &where = g->where;
    vector<vector<int>> &blockInfo = nodeInfo->fileDistribution;
    for(int i = 0; i < g->orgNvtxs; i++) {
        if(where[i] == from) {
            fileDistFrom.insert(fileDistFrom.end(), blockInfo[i].begin(), blockInfo[i].end());
        } else if(where[i] == to) {
            fileDistTo.insert(fileDistTo.end(), blockInfo[i].begin(), blockInfo[i].end());
        }
    }
    // update ckrinfo
    for(int i = 0; i < g->orgNvtxs; i++) {
        if(i == movedVtx) {
            continue;
        }
        int me = where[i], rackId = nodeInfo->networkTopology[i];
        KwayNbrInfo &nowrinfo = g->ckrinfo[i];
        // nowrinfo.ed *= nowrinfo.pNum;
        // if(nowrinfo.nbrs[from] >= nowrinfo.id && me != from) {
        //     nowrinfo.ed -= nowrinfo.nbrs[from];
        //     nowrinfo.pNum--;
        // }
        // if(nowrinfo.nbrs[to] >= nowrinfo.id && me != to) {
        //     nowrinfo.ed -= nowrinfo.nbrs[to];
        //     nowrinfo.pNum--;
        // }
        nowrinfo.nbrs[from] = GetSimilarityForVtx(i, blockInfo[i], fileDistFrom) * 100 * g->tvwgt / g->nvtxs / (blockInfo[i].size() + fileDistFrom.size());
        nowrinfo.nbrs[to] = GetSimilarityForVtx(i, blockInfo[i], fileDistTo) * 100 * g->tvwgt / g->nvtxs / (blockInfo[i].size() + fileDistTo.size());
        if(me == from || me == to) {
            // change id and ed
            nowrinfo.id = nowrinfo.nbrs[me];
            nowrinfo.ed = 0;
            nowrinfo.pNum = 0;
            for(int j = 0; j < g->nvtxs; j++) {
                if(j == me) {
                    continue;
                }
                if(g->topKRacks[j].find(rackId) != g->topKRacks[j].end() && nowrinfo.nbrs[j] >= nowrinfo.id) {
                    nowrinfo.pNum++;
                    nowrinfo.ed += nowrinfo.nbrs[j];
                }
            }
            if(nowrinfo.pNum) {
                nowrinfo.ed /= nowrinfo.pNum;
            }
        } else {
            // if(nowrinfo.nbrs[from] >= nowrinfo.id) {
            //     nowrinfo.ed += nowrinfo.nbrs[from];
            //     nowrinfo.pNum++;
            // }
            // if(nowrinfo.nbrs[to] >= nowrinfo.id) {
            //     nowrinfo.ed += nowrinfo.nbrs[to];
            //     nowrinfo.pNum++;
            // }
            // if(nowrinfo.pNum) {
            //     nowrinfo.ed /= nowrinfo.pNum;
            // }
            nowrinfo.ed = 0;
            nowrinfo.pNum = 0;
            for(int j = 0; j < g->nvtxs; j++) {
                if(j == me) {
                    continue;
                }
                if(g->topKRacks[j].find(rackId) != g->topKRacks[j].end() && nowrinfo.nbrs[j] >= nowrinfo.id) {
                    nowrinfo.pNum++;
                    nowrinfo.ed += nowrinfo.nbrs[j];
                }
            }
            if(nowrinfo.pNum) {
                nowrinfo.ed /= nowrinfo.pNum;
            }
        }
        // update boundary list
        if(omode == OMODE_REFINE) {
            if(bndList.find(i) != bndList.end() && nowrinfo.id >= nowrinfo.ed) {
                bndList.erase(i);
            } else if(bndList.find(i) == bndList.end() && nowrinfo.id < nowrinfo.ed) {
                bndList.insert(i);
            }
        } else if(omode == OMODE_BALANCE) {
            if(bndList.find(i) != bndList.end() && balEIFactor * nowrinfo.id >= nowrinfo.ed) {
                bndList.erase(i);
            } else if(bndList.find(i) == bndList.end() && balEIFactor * nowrinfo.id < nowrinfo.ed) {
                bndList.insert(i);
            }
        } else {
            int rackId = nodeInfo->networkTopology[i];
            if(bndList.find(i) != bndList.end() && g->topKRacks[me].find(rackId) != g->topKRacks[me].end() && netTpEIFactor * nowrinfo.id >= nowrinfo.ed) {
                bndList.erase(i);
            } else if(bndList.find(i) == bndList.end() && g->topKRacks[me].find(rackId) == g->topKRacks[me].end() && netTpEIFactor * nowrinfo.id < nowrinfo.ed) {
                bndList.insert(i);
            }
        }
        
    }

    return;
}

void Combiner::DeleteCoarsenGraph() {
    delete graph->cgraph;
    graph->cgraph = NULL;
    return ;
}

void Combiner::UpdateAdjacencyEdges(Graph *g) {
    vector<vector<int>> fileDist(g->nvtxs);
    vector<vector<int>> &blockInfo = nodeInfo->fileDistribution;
    vector<int> &where = g->where;
    for(int i = 0; i < g->orgNvtxs; i++) {
        fileDist[where[i]].insert(fileDist[where[i]].end(), blockInfo[i].begin(), blockInfo[i].end());
    }
    vector<int> &xadj = g->xadj;
    vector<int> &adjncy = g->adjncy;
    vector<int> &adjwgt = g->adjwgt;
    xadj.clear();
    adjncy.clear();
    adjwgt.clear();
    for(int i = 0; i < g->nvtxs; i++) {
        xadj.push_back(adjncy.size());
        for(int j = 0; j < g->nvtxs; j++) {
            if(j == i) {
                continue;
            }
            int wgt = GetSimilarity(fileDist[i], fileDist[j]) * 100 * 2 * g->tvwgt / g->nvtxs / (fileDist[i].size() + fileDist[j].size());
            if(wgt != 0) {
                adjncy.push_back(j);
                adjwgt.push_back(wgt);
            }
        }
    }
    xadj.push_back(adjncy.size());
    g->nedges = adjncy.size() / 2;
}

void Combiner::AdjustSingleRackNode(Graph *g) {
    if(g->bndList.size() == 0) {
        return ;
    }
    vector<int> &vwgt = g->vwgt;
    vector<int> &where = g->where;
    vector<vector<int>> &blockInfo = nodeInfo->fileDistribution;
    vector<vector<int>> fileDist(g->nvtxs);
    for(int i = 0; i < g->orgNvtxs; i++) {
        fileDist[where[i]].insert(fileDist[where[i]].end(), blockInfo[i].begin(), blockInfo[i].end());
    }
    for(int orgVtxId : g->bndList) {
        int rackId = nodeInfo->networkTopology[orgVtxId], from = where[orgVtxId];
        int maxWgt = -1, to = -1;
        for(int i = 0; i < g->nvtxs; i++) {
            if(i == from) {
                continue;
            }
            if(g->topKRacks[i].find(rackId) != g->topKRacks[i].end()) {
                int wgt = GetSimilarityForVtx(orgVtxId, blockInfo[orgVtxId], fileDist[i]) * 100 * g->tvwgt / g->nvtxs / (blockInfo[orgVtxId].size() + fileDist[i].size());
                if(wgt > maxWgt) {
                    maxWgt = wgt;
                    to = i;
                }
            }
        }
        if(to == -1) {
            // find in second racks
            int maxWgt = -1;
            for(int i = 0; i < g->nvtxs; i++) {
                if(i == from) {
                    continue;
                }
                int vtxCnt = 0;
                for(auto it : g->vracks[i]) {
                    if(it.first == rackId) {
                        vtxCnt = it.second;
                        break;
                    }
                }
                if(vtxCnt > 1) {
                    int wgt = GetSimilarityForVtx(orgVtxId, blockInfo[orgVtxId], fileDist[i]) * 100 * g->tvwgt / g->nvtxs / (blockInfo[orgVtxId].size() + fileDist[i].size());
                    if(wgt > maxWgt) {
                        maxWgt = wgt;
                        to = i;
                    }
                }
            }
        }
        // move the vertex from 'from' to 'to'
        vwgt[to] += g->orgvwgt[orgVtxId];
        vwgt[from] -= g->orgvwgt[orgVtxId];
        where[orgVtxId] = to;
        // update network topology (vRacks) of 'from' and 'to'
        int idx = 0;
        for(; idx < g->vracks[from].size(); idx++) {
            if(g->vracks[from][idx].first == rackId) {
                break;
            }
        }
        int delIdx = idx;
        while(idx < g->vracks[from].size() && g->vracks[from][idx].second == g->vracks[from][delIdx].second) {
            idx++;
        }
        idx--;
        swap(g->vracks[from][idx], g->vracks[from][delIdx]);
        g->vracks[from][idx].second--;
        if(g->vracks[from].back().second == 0) {
            g->vracks[from].pop_back();
        }
        if(delIdx < g->kOfRack && idx >= g->kOfRack) {
            g->topKRacks[from].erase(rackId);
            g->topKRacks[from].insert(g->vracks[from][delIdx].first);
        }

        idx = 0;
        for(; idx < g->vracks[to].size(); idx++) {
            if(g->vracks[to][idx].first == rackId) {
                break;
            }
        }
        if(idx < g->vracks[to].size()) {
            delIdx = idx;
            while(idx >= 0 && g->vracks[to][idx].second == g->vracks[to][delIdx].second) {
                idx--;
            }
            idx++;
            swap(g->vracks[to][idx], g->vracks[to][delIdx]);
            g->vracks[to][idx].second++;
            if(idx < g->kOfRack && delIdx >= g->kOfRack) {
                g->topKRacks[to].erase(g->vracks[to][idx].first);
                g->topKRacks[to].insert(rackId);
            }
        } else {
            // rackId not exist in rackList[to]
            g->vracks[to].push_back(make_pair(rackId, 1));
            if(g->vracks[to].size() <= g->kOfRack) {
                g->topKRacks[to].insert(rackId);
            }
        }
    }
    return ;
}

void Combiner::ShowRackInfo(Graph *g) {
    cout << "debug : rack info" << endl;
    vector<vector<pair<int, int>>> rackInfo(g->nvtxs);
    for(int i = 0; i < g->orgNvtxs; i++) {
        int rackId = nodeInfo->networkTopology[i], vid = g->where[i];
        rackInfo[vid].push_back(make_pair(i, rackId));
    }
    for(auto vtx : rackInfo) {
        for(auto it : vtx) {
            cout << it.first << '(' << it.second << ") ";
        }
        cout << endl;
    }
    return ;
}
