package sjdb;

import java.io.FileReader;

public class TestDriver {

    public static void main(String[] args) throws Exception {

        // 1) 读 catalogue
        Catalogue cat = new Catalogue();
        new CatalogueParser("data/cat.txt", cat).parse();

        // 2) Optimiser+工具
        Optimiser optimiser = new Optimiser(cat);
        Estimator est = new Estimator();
        Inspector insp = new Inspector();

        String[] qs = {"q1.txt","q2.txt","q3.txt","q4.txt","q5.txt"};

        for (String q : qs) {
            System.out.println("========== " + q + " ==========");
            QueryParser qp = new QueryParser(cat, new FileReader("data/" + q));
            Operator canonical = qp.parse();

            // 原始
            canonical.accept(est);
            System.out.println("-- canonical --");
            canonical.accept(insp);

            // 仅 Join-化
            Operator joinPlan = optimiser.optimise(canonical);
            joinPlan.accept(est);
            System.out.println("-- join-rewritten --");
            joinPlan.accept(insp);
            System.out.println();
        }
    }
}
