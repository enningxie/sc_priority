package com.souche;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Warning {

    // 路径的叶子节点列表
    private static final List<Integer> LAST_NODES = new ArrayList<Integer>(Arrays.asList(9, 10, 11, 12));

    // 各个路径顺序节点列表
    private static final List<Integer> pathList_0 = new ArrayList<Integer>(Arrays.asList(0, 1, 3, 5, 6, 9));

    private static final List<Integer> pathList_1 = new ArrayList<Integer>(Arrays.asList(0, 1, 3, 4, 7, 8, 10));

    private static final List<Integer> pathList_2 = new ArrayList<Integer>(Arrays.asList(0, 1, 3, 4, 11));

    private static final List<Integer> pathList_3 = new ArrayList<Integer>(Arrays.asList(0, 2, 12));

    private static final List<List<Integer>> pathList = new ArrayList<List<Integer>>(Arrays.asList(pathList_0, pathList_1, pathList_2, pathList_3));

    // 各个路径顺序节点到达时间列表
    private static final List<Double> pathTime_0 = new ArrayList<Double>(Arrays.asList(1.44, 1.3, 0.93, 0.31, 1.55));

    private static final List<Double> pathTime_1 = new ArrayList<Double>(Arrays.asList(1.44, 1.3, 1.18, 1.01, 0.3, 0.66));

    private static final List<Double> pathTime_2 = new ArrayList<Double>(Arrays.asList(1.44, 1.3, 1.18, 1.9));

    private static final List<Double> pathTime_3 = new ArrayList<Double>(Arrays.asList(1.84, 3.62));

    private static final List<List<Double>> pathTime = new ArrayList<List<Double>>(Arrays.asList(pathTime_0, pathTime_1, pathTime_2, pathTime_3));

    // 用于计算概率
    private static final List<String> pathColumn_0 = new ArrayList<String>(Arrays.asList("L_2", "L_3", "L_9", "L_10", "L_11"));

    private static final List<String> pathColumn_1 = new ArrayList<String>(Arrays.asList("L_2", "L_3", "L_4", "L_6", "L_7", "L_8"));

    private static final List<String> pathColumn_2 = new ArrayList<String>(Arrays.asList("L_2", "L_3", "L_4", "L_5"));

    private static final List<String> pathColumn_3 = new ArrayList<String>(Arrays.asList("L_0", "L_1"));

    private static final List<List<String>> pathColumn = new ArrayList<List<String>>(Arrays.asList(pathColumn_0, pathColumn_1, pathColumn_2, pathColumn_3));

    // 缓存的 csv数据用于计算概率
    private static final String PATH = "C:\\Users\\ennin\\Desktop\\work\\Codes\\warnning_\\data\\normal_data.csv";

    // 路径节点集合
    private static final Set<Integer> pathIndex_0 = new HashSet<Integer>(){
        {
            add(0);
            add(1);
            add(3);
            add(5);
            add(6);
            add(9);
        }
    };

    private static final Set<Integer> pathIndex_1 = new HashSet<Integer>(){
        {
            add(0);
            add(1);
            add(3);
            add(4);
            add(7);
            add(8);
            add(10);
        }
    };

    private static final Set<Integer> pathIndex_2 = new HashSet<Integer>(){
        {
            add(0);
            add(1);
            add(3);
            add(4);
            add(11);
        }
    };

    private static final Set<Integer> pathIndex_3 = new HashSet<Integer>(){
        {
            add(0);
            add(2);
            add(12);
        }
    };

    // 各个路径节点集合的列表
    private static final List<Set<Integer>> pathIndex =
            new ArrayList<Set<Integer>>(Arrays.asList(pathIndex_0, pathIndex_1, pathIndex_2, pathIndex_3));
    

    // 初始化优先级栈
    public static Stack<Set<Integer>> initStack(){
        Stack<Set<Integer>> propertyStack = new Stack<Set<Integer>>();
        Set<Integer> property_1 = new HashSet<Integer>(){{
            add(0);
        }};

        Set<Integer> property_2 = new HashSet<Integer>(){{
            add(1);
            add(2);
            add(3);
        }};

        Set<Integer> property_3 = new HashSet<Integer>(){{
            add(4);
            add(5);
            add(6);
        }};

        Set<Integer> property_4 = new HashSet<Integer>(){{
            add(7);
            add(8);
        }};

        Set<Integer> property_5 = new HashSet<Integer>(){{
            add(9);
            add(10);
            add(11);
            add(12);
        }};

        propertyStack.push(property_5);
        propertyStack.push(property_4);
        propertyStack.push(property_3);
        propertyStack.push(property_2);
        propertyStack.push(property_1);
        return propertyStack;
    }

    // 过滤初步警报节点，保留每条路径上优先级最高的节点
    public static Set<Integer> filterResult(Set<Integer> result){
        Set<Integer> returnSet = new HashSet<Integer>();
        Set<Integer> tmpSet = new HashSet<Integer>();
        Set<Integer> tmpMinSet = new HashSet<Integer>();
        for(Set<Integer> path: pathIndex){
            tmpSet.clear();
            tmpSet.addAll(result);
            tmpSet.retainAll(path);
            if(tmpSet.size() > 1){
                int tmpMin = 9;
                for(int iNode: tmpSet){
                    if(iNode < tmpMin){
                        tmpMin = iNode;
                    }
                }
                tmpMinSet.clear();
                tmpMinSet.add(tmpMin);
                returnSet.addAll(tmpMinSet);
            }else {
                returnSet.addAll(tmpSet);
            }
        }
        return returnSet;
    }


    // 对到达时间列表进行排序，返回排序后节点索引
    public static int[] argsort(final double[] a, final boolean ascending) {
        int count = countNan(a);
        Integer[] indexes = new Integer[a.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }
        Arrays.sort(indexes, new Comparator<Integer>() {
            public int compare(final Integer i1, final Integer i2) {
                return (ascending ? 1 : -1) * Double.compare(a[i1], a[i2]);
            }
        });
        return asArray(count, indexes);
    }

    public static <T extends Number> int[] asArray(int count, final T... a) {
        int[] b = new int[count];
        for (int i = 0; i < b.length; i++) {
            b[i] = a[i].intValue();
        }
        return b;
    }

    public static int countNan(double[] oneLineData){
        int count = 0;
        for (double i : oneLineData){
            if (!Double.isNaN(i)){
                ++count;
            }
        }
        return count;
    }

    // 获取当前警告节点⚠
    public static Set<Integer> warning(int[] sortedNode, Stack<Set<Integer>> propertyStack, double[] nodeTimes, String startTime) throws Exception{
        Set<Integer> resultSet = new HashSet<Integer>();
        Set<Integer> tmp_set = new HashSet<Integer>();

        // 用于判断有没有路径完成
        boolean notFinished = true;
        // 用于记录首次到达的节点
        int firstNode = -1;
        for (int node : sortedNode){
            // 判断当前node 有没有被移除
            boolean removed = false;
            while (!removed){
                // 当报警节点已经响应，从报警集合中删除
                if (resultSet.contains(node)){
                    resultSet.remove(node);
                }
                // 判断当前node 是否在栈顶集合
                if (!propertyStack.isEmpty() && propertyStack.peek().contains(node)){
                    propertyStack.peek().remove(node);
                    removed = true;
                    // 判断移除的节点是否是路径叶子节点
                    if (notFinished && LAST_NODES.contains(node)){
                        notFinished = false;
                        firstNode = node;
                    }
                }
                // tmp_set 用于存放报警节点
                else if (!tmp_set.contains(node)){
                    Set<Integer> popSet = propertyStack.pop();
                    resultSet.addAll(popSet);
                    tmp_set.addAll(popSet);
                }
                else {
                    removed = true;
                }
                // 当栈不为空，且栈顶集合为空，移除空集合
                if (!propertyStack.isEmpty() && propertyStack.peek().isEmpty() ){
                    propertyStack.pop();
                }
            }
        }
        Set<Integer> resultWarning = filterResult(resultSet);
        if (!notFinished){
            // 获取当前报警节点集合
            Set<Integer> currWarning = filterResult(resultSet);
            for (int warningNode : currWarning){
                // 遍历各个路径
                List<Double> remainTimeList = new ArrayList<Double>();
                boolean removeWarning = true;
                int pathListIndex = 0;
                for (List<Integer> pathList_ : pathList){
                    if (pathList_.contains(warningNode)){
                        // 获取当前节点索引
                        int warningNodeIndex = pathList_.indexOf(warningNode);
                        // plan A
                        // 差值
                        // nodeTimes[sortedNode[sortedNode.length-1]] todo 需要用now替换
                        double delta = pathTime.get(pathListIndex).get(warningNodeIndex-1) -
                                (nodeTimes[sortedNode[sortedNode.length-1]] - nodeTimes[pathList_.get(warningNodeIndex-1)]);
                        double partSum = 0;
                        int partTimeLen = pathTime.get(pathListIndex).size();
                        for (int i = warningNodeIndex; i < partTimeLen; ++i){
                            partSum += pathTime.get(pathListIndex).get(i);
                        }
                        double delta_2 = nodeTimes[sortedNode[sortedNode.length-1]] - nodeTimes[firstNode];
                        double remainTime = 3 - (Math.max(0, delta) + partSum + delta_2);
                        remainTimeList.add(remainTime);
                        // plan B
                        // nodeTimes[sortedNode[sortedNode.length-1]] todo 需要用now替换
                        double probability_ = delayProbability(pathListIndex, warningNodeIndex, nodeTimes[sortedNode[sortedNode.length-1]],
                                nodeTimes[pathList_.get(warningNodeIndex-1)], nodeTimes[firstNode]);

                        System.out.println("Path: " + pathListIndex + ", node num: " + warningNode + ", probability: " + probability_);

                    }
                    ++ pathListIndex;
                }

                if (!remainTimeList.isEmpty()){
                    for (double time_ : remainTimeList){
                        if (time_ < 0){
                            removeWarning = false;
                        }
                    }
                    if (removeWarning){
                        resultWarning.remove(warningNode);
                    }
                }
            }
        }
        System.out.print("----------------------------------");
        return resultWarning;
    }

    public static double delayProbability (int pathListIndex, int warningNodeIndex, double nowTime, double nodeTime, double earlyTime) throws Exception{
        double probability = -1.0;
        Reader reader = Files.newBufferedReader(Paths.get(PATH));
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(reader);

        List<String> selectPathColumn = pathColumn.get(pathListIndex);
        String current_l = selectPathColumn.get(warningNodeIndex - 1);
        List<String> remain_l = new ArrayList<>();
        for (int i = warningNodeIndex; i < selectPathColumn.size(); ++i){
            remain_l.add(selectPathColumn.get(i));
        }

        double pastTime = nowTime - earlyTime;
        double remainTimeMax = 3 - pastTime;
        double nodePastTime = nowTime - nodeTime;

        List<Double> current_l_df = new ArrayList<>();
        // Map<String, Double> remain_l_df = new HashMap<>();
        List<Double> resultList = new ArrayList<>();
        List<Double> sumDf = new ArrayList<>();

        int tmpCount_1 = 0;
        for (CSVRecord record : records) {
            String L_0 = record.get(current_l);
            double tmpData_1 = Double.parseDouble(L_0) / 24;
            tmpData_1 -= nodePastTime;
            tmpData_1 = Math.max(0, tmpData_1);
            current_l_df.add(tmpData_1);

            int tmpCount_2 = 0;
            double tmpSum = 0;
            for (String tmpName: remain_l){
                String tmpData = record.get(tmpName);
                tmpSum += Double.parseDouble(tmpData) / 24;
                if (tmpCount_2 == remain_l.size() - 1){
                    resultList.add(tmpSum);
                }
                ++ tmpCount_2;
            }
            sumDf.add(tmpData_1 + resultList.get(tmpCount_1));
            ++ tmpCount_1;
        }


        double[] sumDfDouble = new double[sumDf.size()];
        StandardDeviation std = new StandardDeviation();
        Mean mean = new Mean();

        int i = 0;
        for (Double tmpData: sumDf){
            sumDfDouble[i] = tmpData;
            i++;
        }

        double sumDfStd = std.evaluate(sumDfDouble);
        double sumDfMean = mean.evaluate(sumDfDouble);

        NormalDistribution normalDis = new NormalDistribution();

        if (remainTimeMax <= 0){
            probability = 1.0;
        }
        else {
            double z_score = (remainTimeMax - sumDfMean) / sumDfStd;
            probability = 1 - normalDis.cumulativeProbability(z_score);
        }

        return probability;
    }


    public static void main(String[] args) throws Exception{
        Warning test = new Warning();


//        // test initStack()
//        // 遍历优先级栈
//        Stack<Set<Integer>> propertyStack_ = test.initStack();
//        while(!propertyStack_.isEmpty()){
//            Set<Integer> tmpData = propertyStack_.pop();
//            Iterator it=tmpData.iterator();
//            while(it.hasNext()){
//                System.out.println(it.next());
//            }
//            System.out.println();
//        }


//        // test filterResult(Set<Integer> result)
//        Set<Integer> testFilter = new HashSet<Integer>(){{
//            add(5);
//            add(6);
//        }};
//
//        Set<Integer> resultSet = test.filterResult(testFilter);
//
//        for(int i : resultSet){
//            System.out.print(i);
//        }

        // test warning
        double[] test_data = {7.7, 21.86, 7.88, 22.53, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 19.71};
        String start_time = "";
        int[] need_index = argsort(test_data, true);
        Stack<Set<Integer>> property_stack_ = initStack();
        Set<Integer> result = warning(need_index, property_stack_, test_data, start_time);

        for (int i : result){
            System.out.println();
            System.out.println(i);
        }

    }
}

