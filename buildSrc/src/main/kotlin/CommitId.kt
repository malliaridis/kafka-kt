import org.gradle.api.Project
import java.io.File

fun determineCommitId(project: Project, rootDir: File): String {
    val takeFromHash = 16

    val commitId = project.findProperty("commitId")
    return if (commitId is String) commitId.take(takeFromHash)
    else if (File("$rootDir/.git/HEAD").exists()) {
        var headRef = File("$rootDir/.git/HEAD").readText()
        return if (headRef.contains("ref: ")) {
            headRef = headRef.replace("ref: ", "").trim()
            if (File("$rootDir/.git/$headRef").exists())
                File("$rootDir/.git/$headRef").readText().trim().take(takeFromHash)
            else headRef.trim().take(takeFromHash) // TODO Check if this addition is correct
        } else headRef.trim().take(takeFromHash)
    } else "unknown"
}
