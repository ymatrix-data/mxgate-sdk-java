package runner;

public interface E2ECase {
    void execute() throws Exception;
    String getName();
}
