


using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using CSVDataIngestionTest.Models;

namespace TestApplication.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CsvController : ControllerBase
    {
        private readonly string _connectionString = "Server=DESKTOP-RDT234N\\SQLEXPRESS;Database=CsvDatabaseTest1;Trusted_Connection=True;Integrated Security=True;TrustServerCertificate=True;"; // Replace with your actual connection string

        [HttpPost("IngestCsv")]
        public async Task<IActionResult> IngestCsv(IFormFile file)
        {
            try
            {
                if (file != null && file.Length > 0)
                {
                    using (var stream = new StreamReader(file.OpenReadStream()))
                    {
                        List<Student> students = new List<Student>();
                        string line;
                        while ((line = await stream.ReadLineAsync()) != null)
                        {
                            string[] values = line.Split(',');

                            Student student = new Student
                            {
                                Student_ID = values[0],
                                Gender = values[1],
                                Nationlity = values[2],
                                PlaceOfBirth = values[3],
                                StageID = values[4],
                                GradeID = values[5],
                                SectionID = values[6],
                                Topic = values[7],
                                Semester = values[8],
                                Relation = values[9],
                                ParentAnsweringSurvey = values[14],
                                ParentschoolSatisfaction = values[15],
                                StudentAbsenceDays = values[16],
                                Classes = values[18]
                            };

                          //  Handling numeric properties
                            int raisedHands, visitedResources, announcementsView, discussion, studentMarks;
                            if (int.TryParse(values[10], out raisedHands))
                                student.RaisedHands = raisedHands;
                            else
                                student.RaisedHands = -1; // Default value or handle as needed

                            if (int.TryParse(values[11], out visitedResources))
                                student.VisitedResources = visitedResources;
                            else
                                student.VisitedResources = -1; // Default value or handle as needed

                            if (int.TryParse(values[12], out announcementsView))
                                student.AnnouncementsView = announcementsView;
                            else
                                student.AnnouncementsView = -1; // Default value or handle as needed

                            if (int.TryParse(values[13], out discussion))
                                student.Discussion = discussion;
                            else
                                student.Discussion = -1; // Default value or handle as needed

                            if (int.TryParse(values[17], out studentMarks))
                                student.StudentMarks = studentMarks;
                            else
                                student.StudentMarks = -1; // Default value or handle as needed

                            students.Add(student);
                        }

                        await InsertStudentsIntoDatabase(students);

                        return Ok("CSV data ingested successfully.");
                    }
                }
                else
                {
                    return BadRequest("Invalid file or empty file.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

     

        private async Task InsertStudentsIntoDatabase(List<Student> students)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    try
                    {
                        foreach (var student in students)
                        {
                            using (SqlCommand command = new SqlCommand("INSERT INTO Students1 (Student_ID, Gender, Nationlity, PlaceOfBirth, StageID, GradeID, SectionID, Topic, Semester, Relation, RaisedHands, VisitedResources, AnnouncementsView, Discussion, ParentAnsweringSurvey, ParentschoolSatisfaction, StudentAbsenceDays, StudentMarks, Classes) VALUES (@Student_ID, @Gender, @Nationlity, @PlaceOfBirth, @StageID, @GradeID, @SectionID, @Topic, @Semester, @Relation, @RaisedHands, @VisitedResources, @AnnouncementsView, @Discussion, @ParentAnsweringSurvey, @ParentschoolSatisfaction, @StudentAbsenceDays, @StudentMarks, @Classes)", connection, transaction))
                            {
                                command.Parameters.AddWithValue("@Student_ID", student.Student_ID);
                                command.Parameters.AddWithValue("@Gender", student.Gender);
                                command.Parameters.AddWithValue("@Nationlity", student.Nationlity);
                                command.Parameters.AddWithValue("@PlaceOfBirth", student.PlaceOfBirth);
                                command.Parameters.AddWithValue("@StageID", student.StageID);
                                command.Parameters.AddWithValue("@GradeID", student.GradeID);
                                command.Parameters.AddWithValue("@SectionID", student.SectionID);
                                command.Parameters.AddWithValue("@Topic", student.Topic);
                                command.Parameters.AddWithValue("@Semester", student.Semester);
                                command.Parameters.AddWithValue("@Relation", student.Relation);
                                command.Parameters.AddWithValue("@RaisedHands", student.RaisedHands);
                                command.Parameters.AddWithValue("@VisitedResources", student.VisitedResources);
                                command.Parameters.AddWithValue("@AnnouncementsView", student.AnnouncementsView);
                                command.Parameters.AddWithValue("@Discussion", student.Discussion);
                                command.Parameters.AddWithValue("@ParentAnsweringSurvey", student.ParentAnsweringSurvey);
                                command.Parameters.AddWithValue("@ParentschoolSatisfaction", student.ParentschoolSatisfaction);
                                command.Parameters.AddWithValue("@StudentAbsenceDays", student.StudentAbsenceDays);
                                command.Parameters.AddWithValue("@StudentMarks", student.StudentMarks);
                                command.Parameters.AddWithValue("@Classes", student.Classes);

                                await command.ExecuteNonQueryAsync();
                            }
                        }

                        transaction.Commit();
                    }
                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }
        }


    }
}
